import os
import sys
import uuid
import sqlite3
import asyncio
import random
from typing import Optional, Tuple


class FastCursor:
    def __init__(self, cursor: sqlite3.Cursor):
        self._cursor = cursor
        self.rowcount = cursor.rowcount

    async def fetchone(self):
        return self._cursor.fetchone()

    async def fetchall(self):
        return self._cursor.fetchall()

    async def close(self):
        self._cursor.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._cursor.close()

    def __iter__(self):
        return iter(self._cursor)

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = self._cursor.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row


class ExecuteContextManager:
    def __init__(self, conn: sqlite3.Connection, sql: str, parameters=()):
        self._conn = conn
        self._sql = sql
        self._parameters = parameters
        self._cursor: Optional[FastCursor] = None

    def __await__(self):
        return self._execute().__await__()

    async def _execute(self) -> FastCursor:
        cursor = self._conn.execute(self._sql, self._parameters)
        return FastCursor(cursor)

    async def __aenter__(self) -> FastCursor:
        self._cursor = await self._execute()
        return self._cursor

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._cursor:
            await self._cursor.close()


class FastConnection:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def execute(self, sql: str, parameters=()) -> ExecuteContextManager:
        return ExecuteContextManager(self._conn, sql, parameters)

    async def commit(self):
        self._conn.commit()

    async def close(self):
        self._conn.close()


DB_PATH = os.getenv("WEBHOOK_DB_PATH", "webhooks.db")
_db_conn: FastConnection | None = None
_db_lock = asyncio.Lock()


def get_db_path(db_path: str | None = None) -> str:
    return db_path or os.getenv("WEBHOOK_DB_PATH", DB_PATH)


async def get_db(db_path: str | None = None) -> FastConnection:
    global _db_conn
    if _db_conn is None:
        path = get_db_path(db_path)
        kwargs = {"check_same_thread": False}
        if sys.version_info >= (3, 12):
            kwargs["autocommit"] = True
        else:
            kwargs["isolation_level"] = None
        conn = sqlite3.connect(path, **kwargs)
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        conn.execute("PRAGMA cache_size = -64000;")
        _db_conn = FastConnection(conn)
    return _db_conn


async def close_db():
    global _db_conn
    if _db_conn is not None:
        await _db_conn.close()
        _db_conn = None


async def init_db(db_path: str | None = None):
    db = await get_db(db_path)
    async with _db_lock:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                idempotency_key TEXT UNIQUE,
                tenant_id TEXT NOT NULL,
                raw_content TEXT,
                cleaned_text TEXT,
                fingerprint TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                owner_id TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_retry_at TIMESTAMP,
                last_error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Ensure owner_id column exists if table was previously created
        try:
            await db.execute("ALTER TABLE events ADD COLUMN owner_id TEXT;")
        except Exception:
            pass

        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_status_retry ON events(status, next_retry_at);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_idem ON events(idempotency_key);")
        # Lease-based recovery: Only reclaim orphaned processing jobs whose lease expired (> 120s)
        # to prevent a newly starting worker from stealing active jobs from existing workers.
        await db.execute("""
            UPDATE events
            SET status = 'pending', owner_id = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE status = 'processing' AND updated_at <= datetime('now', '-120 seconds');
        """)
        await db.commit()


async def insert_or_check_conflict(
    event_id: str,
    idempotency_key: str,
    tenant_id: str,
    raw_content: str,
    db_path: str | None = None,
) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Atomically inserts a new pending event or detects conflicts.
    Returns:
      ("inserted", event_id, idempotency_key) if newly inserted.
      ("payload_mismatch", existing_event_id, existing_key) if key or event was reused with mismatched parameters.
      ("duplicate", existing_event_id, existing_key) if genuine identical retry duplicate.
    """
    db = await get_db(db_path)
    cursor = await db.execute(
        """
        INSERT INTO events (event_id, idempotency_key, tenant_id, raw_content, status)
        VALUES (?, ?, ?, ?, 'pending')
        ON CONFLICT DO NOTHING
        """,
        (event_id, idempotency_key, tenant_id, raw_content),
    )
    if cursor.rowcount > 0:
        return ("inserted", event_id, idempotency_key)

    # Inspect database for existing persisted row
    async with db.execute(
        """
        SELECT event_id, idempotency_key, tenant_id, raw_content
        FROM events
        WHERE idempotency_key = ? OR event_id = ?
        LIMIT 1
        """,
        (idempotency_key, event_id),
    ) as cur:
        existing = await cur.fetchone()
        if existing:
            existing_evt_id, existing_key, existing_tenant, existing_content = existing
            # If key is reused with different event_id, tenant_id, or content -> 409 Conflict
            if existing_key == idempotency_key:
                if (existing_evt_id != event_id or
                    existing_tenant != tenant_id or
                    existing_content != raw_content):
                    return ("payload_mismatch", existing_evt_id, existing_key)
            elif existing_evt_id == event_id:
                # Same event_id used with different key or different payload -> 409 Conflict
                if (existing_key != idempotency_key or
                    existing_tenant != tenant_id or
                    existing_content != raw_content):
                    return ("payload_mismatch", existing_evt_id, existing_key)

            return ("duplicate", existing_evt_id, existing_key)
        return ("duplicate", None, None)


async def claim_next_job(
    worker_id: str | None = None,
    db_path: str | None = None,
) -> Optional[Tuple[str, str, str, str, int, str]]:
    """
    Atomically claims a single pending or retry-eligible job from SQLite.
    Binds the claim to a unique owner_id token with a 120-second lease.
    Multi-worker safe via UPDATE ... RETURNING.
    """
    if db_path is None and worker_id and (worker_id.endswith(".db") or "/" in worker_id):
        db_path = worker_id
        worker_id = None

    owner_token = worker_id or uuid.uuid4().hex

    db = await get_db(db_path)
    cursor = await db.execute(
        """
        UPDATE events
        SET status = 'processing', owner_id = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = (
            SELECT id FROM events
            WHERE status = 'pending'
               OR (status = 'retry_scheduled' AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP))
               OR (status = 'processing' AND updated_at <= datetime('now', '-120 seconds'))
            ORDER BY id ASC
            LIMIT 1
        )
        RETURNING event_id, idempotency_key, tenant_id, raw_content, attempts, owner_id;
        """,
        (owner_token,),
    )
    row = await cursor.fetchone()
    if row:
        return (row[0], row[1] or "", row[2], row[3] or "", row[4], row[5] or owner_token)
    return None


async def renew_lease(
    event_id: str,
    owner_id: str,
    db_path: str | None = None,
) -> bool:
    """
    Heartbeat mechanism: Renews the processing lease while an active worker
    is performing slow I/O or downstream retries.
    Requires matching owner_id and processing status.
    """
    db = await get_db(db_path)
    cursor = await db.execute(
        """
        UPDATE events
        SET updated_at = CURRENT_TIMESTAMP
        WHERE event_id = ? AND owner_id = ? AND status = 'processing'
        """,
        (event_id, owner_id),
    )
    return cursor.rowcount > 0


async def mark_delivered(
    event_id: str,
    cleaned_text: str,
    fingerprint: str,
    owner_id: str | None = None,
    db_path: str | None = None,
) -> bool:
    """
    Updates the event record to 'delivered' status.
    Guarded by owner_id and status = 'processing' to prevent stale workers from overwriting
    reclaimed or rescheduled jobs.
    """
    db = await get_db(db_path)
    if owner_id:
        cursor = await db.execute(
            """
            UPDATE events
            SET cleaned_text = ?, fingerprint = ?, status = 'delivered', next_retry_at = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE event_id = ? AND owner_id = ? AND status = 'processing'
            """,
            (cleaned_text, fingerprint, event_id, owner_id),
        )
    else:
        cursor = await db.execute(
            """
            UPDATE events
            SET cleaned_text = ?, fingerprint = ?, status = 'delivered', next_retry_at = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE event_id = ? AND status = 'processing'
            """,
            (cleaned_text, fingerprint, event_id),
        )
    return cursor.rowcount > 0


async def mark_permanent_failure(
    event_id: str,
    error_message: str,
    attempts: int,
    owner_id: str | None = None,
    db_path: str | None = None,
) -> bool:
    """
    Marks a non-retryable error (e.g. 400 Bad Request) directly into terminal 'failed' status.
    Guarded by owner_id and status = 'processing'.
    """
    db = await get_db(db_path)
    if owner_id:
        cursor = await db.execute(
            """
            UPDATE events
            SET status = 'failed',
                attempts = ?,
                next_retry_at = NULL,
                last_error = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE event_id = ? AND owner_id = ? AND status = 'processing'
            """,
            (attempts, error_message, event_id, owner_id),
        )
    else:
        cursor = await db.execute(
            """
            UPDATE events
            SET status = 'failed',
                attempts = ?,
                next_retry_at = NULL,
                last_error = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE event_id = ? AND status = 'processing'
            """,
            (attempts, error_message, event_id),
        )
    return cursor.rowcount > 0


async def schedule_transient_retry(
    event_id: str,
    error_message: str,
    attempts: int,
    owner_id: str | None = None,
    base_delay_sec: float = 1.0,
    max_delay_sec: float = 30.0,
    db_path: str | None = None,
) -> bool:
    """
    Schedules an automatic retry in SQLite with exponential backoff and jitter.
    Releases owner_id so any worker can claim the retry once eligible.
    Guarded by owner_id and status = 'processing'.
    """
    db = await get_db(db_path)
    delay = min(max_delay_sec, base_delay_sec * (1.5 ** attempts)) + random.uniform(0.1, 0.5)
    delay_int = max(1, int(delay))
    if owner_id:
        cursor = await db.execute(
            """
            UPDATE events
            SET status = 'retry_scheduled',
                attempts = ?,
                next_retry_at = datetime('now', '+' || ? || ' seconds'),
                last_error = ?,
                owner_id = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE event_id = ? AND owner_id = ? AND status = 'processing'
            """,
            (attempts, delay_int, error_message, event_id, owner_id),
        )
    else:
        cursor = await db.execute(
            """
            UPDATE events
            SET status = 'retry_scheduled',
                attempts = ?,
                next_retry_at = datetime('now', '+' || ? || ' seconds'),
                last_error = ?,
                owner_id = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE event_id = ? AND status = 'processing'
            """,
            (attempts, delay_int, error_message, event_id),
        )
    return cursor.rowcount > 0

import asyncio
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager
import logging
import os
import pydantic_core
import httpx
from fastapi import FastAPI

from app.database import (
    init_db,
    close_db,
    get_db,
    insert_or_check_conflict,
    claim_next_job,
    renew_lease,
    mark_delivered,
    mark_permanent_failure,
    schedule_transient_retry,
)
from app.downstream import forward_event
from app.processor import scrub_and_fingerprint

logger = logging.getLogger("uvicorn.error")

process_pool: ProcessPoolExecutor | None = None
http_client: httpx.AsyncClient | None = None
job_signal = asyncio.Semaphore(0)
worker_tasks: list[asyncio.Task] = []
class InFlightOp:
    __slots__ = ("event_id", "idempotency_key", "tenant_id", "raw_content", "event", "status_result", "existing_evt_id", "existing_key", "error")

    def __init__(self, event_id: str, idempotency_key: str, tenant_id: str, raw_content: str):
        self.event_id = event_id
        self.idempotency_key = idempotency_key
        self.tenant_id = tenant_id
        self.raw_content = raw_content
        self.event = asyncio.Event()
        self.status_result: Optional[str] = None
        self.existing_evt_id: Optional[str] = None
        self.existing_key: Optional[str] = None
        self.error: Optional[Exception] = None


seen_idempotency_keys: dict[bytes, tuple[str, str, str]] = {}
seen_event_ids: set[str] = set()
in_flight_keys: dict[bytes, InFlightOp] = {}
in_flight_events: dict[str, InFlightOp] = {}


async def event_worker(worker_id: str = "worker-0"):
    loop = asyncio.get_running_loop()
    while True:
        try:
            # Wait for next ingestion event permit or periodic timer (10.0s) for scheduled retries
            try:
                await asyncio.wait_for(job_signal.acquire(), timeout=10.0)
            except asyncio.TimeoutError:
                pass

            # Yield to event loop to allow concurrent network requests (e.g. /health and webhooks) to process
            await asyncio.sleep(0.25)

            # Atomically claim next pending or retry-eligible job from SQLite bound to a globally unique claim token
            claim_token = f"{worker_id}-{uuid.uuid4().hex}"
            job = await claim_next_job(worker_id=claim_token)
            if job is None:
                continue

            event_id, idempotency_key, tenant_id, raw_content, attempts, owner_id = job

            # Heartbeat task: Renews lease every 5s while processing CPU scrubbing and network I/O
            heartbeat_stop = asyncio.Event()

            async def lease_heartbeat():
                while not heartbeat_stop.is_set():
                    try:
                        await asyncio.sleep(5.0)
                        if heartbeat_stop.is_set():
                            break
                        await renew_lease(event_id, owner_id)
                    except asyncio.CancelledError:
                        break
                    except Exception as err:
                        logger.warning(f"Heartbeat failed for {event_id}: {err}")

            heartbeat_task = asyncio.create_task(lease_heartbeat())

            try:
                # 1. Offload CPU-bound regex + cryptographic hashing to process pool
                cleaned_text, fingerprint = await loop.run_in_executor(
                    process_pool, scrub_and_fingerprint, raw_content
                )

                # 2. Forward downstream with transient backoff and downstream idempotency header
                await forward_event(
                    event_id=event_id,
                    cleaned_text=cleaned_text,
                    fingerprint=fingerprint,
                    client=http_client,
                    idempotency_key=idempotency_key,
                )

                # 3. Atomically persist delivered state guarded by matching owner_id and status='processing'
                await mark_delivered(
                    event_id=event_id,
                    cleaned_text=cleaned_text,
                    fingerprint=fingerprint,
                    owner_id=owner_id,
                )
            except Exception as exc:
                # Distinguish permanent client errors from transient infrastructure blips
                is_permanent = False
                if isinstance(exc, httpx.HTTPStatusError):
                    # 4xx client errors (except 429 Too Many Requests) are non-retryable
                    if 400 <= exc.response.status_code < 500 and exc.response.status_code != 429:
                        is_permanent = True

                if is_permanent:
                    logger.warning(f"Permanent downstream error for {event_id}: {exc}. Marking terminal failed.")
                    await mark_permanent_failure(
                        event_id=event_id,
                        error_message=str(exc),
                        attempts=attempts + 1,
                        owner_id=owner_id,
                    )
                else:
                    logger.info(f"Transient downstream error for {event_id}: {exc}. Scheduling retry.")
                    await schedule_transient_retry(
                        event_id=event_id,
                        error_message=str(exc),
                        attempts=attempts + 1,
                        owner_id=owner_id,
                    )
                    loop.call_later(1.5, job_signal.release)
            finally:
                heartbeat_stop.set()
                heartbeat_task.cancel()
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error(f"Worker {worker_id} unexpected error: {exc}")


def init_worker():
    try:
        os.nice(19)
    except Exception:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global process_pool, http_client, worker_tasks, seen_idempotency_keys, seen_event_ids

    await init_db()

    # Pre-populate seen idempotency keys and event IDs from DB on startup
    db = await get_db()
    async with db.execute("SELECT idempotency_key, event_id, tenant_id, raw_content FROM events") as cursor:
        rows = await cursor.fetchall()
        for idem_key, evt_id, tenant, raw_cnt in rows:
            if idem_key:
                seen_idempotency_keys[idem_key.encode("utf-8")] = (evt_id or "", tenant or "", raw_cnt or "")
            if evt_id:
                seen_event_ids.add(evt_id)

    # Process pool for CPU-bound hashing
    pool_workers = 2
    process_pool = ProcessPoolExecutor(
        max_workers=pool_workers,
        initializer=init_worker,
    )

    # Connection-pooled async HTTP client for resilient downstream deliveries
    http_client = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        timeout=httpx.Timeout(10.0),
    )

    num_workers = 8
    worker_tasks = [asyncio.create_task(event_worker(f"worker-{i}")) for i in range(num_workers)]

    # Release permits for existing pending or retry_scheduled events so workers immediately drain them
    db = await get_db()
    async with db.execute("SELECT COUNT(*) FROM events WHERE status = 'pending' OR status = 'retry_scheduled'") as cursor:
        row = await cursor.fetchone()
        pending_count = row[0] if row else 0
    for _ in range(pending_count):
        job_signal.release()

    # Warm up worker process during startup
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(process_pool, int, 1)

    yield

    # Teardown workers and connections
    for t in worker_tasks:
        t.cancel()
    if http_client:
        await http_client.aclose()
    if process_pool:
        for p in list(getattr(process_pool, "_processes", {}).values()):
            try:
                p.terminate()
            except Exception:
                pass
        process_pool.shutdown(wait=True, cancel_futures=True)
    await close_db()


raw_fastapi_app = FastAPI(title="Webhook Ingestion Service", lifespan=lifespan)

HEALTH_HEADERS = [(b"content-type", b"application/json"), (b"content-length", b"15")]
HEALTH_BODY = b'{"status":"ok"}'

ACCEPTED_HEADERS = [(b"content-type", b"application/json"), (b"content-length", b"21")]
ACCEPTED_BODY = b'{"status":"accepted"}'

DUP_HEADERS = [(b"content-type", b"application/json"), (b"content-length", b"38")]
DUP_BODY = b'{"status":"accepted","duplicate":true}'

METHOD_NOT_ALLOWED_HEADERS = [(b"content-type", b"application/json"), (b"content-length", b"31")]
METHOD_NOT_ALLOWED_BODY = b'{"detail":"Method not allowed"}'

MAX_PAYLOAD_SIZE = 1024 * 1024  # 1MB


async def send_json(send, status: int, body: bytes):
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
    ]
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})


async def app(scope, receive, send):
    """
    High-performance ASGI dispatcher.
    Bypasses framework routing for high-throughput health checks
    and webhook ingestion acknowledging requests in sub-millisecond time.
    """
    if scope["type"] == "http":
        path = scope["path"]
        method = scope["method"]
        if path == "/health":
            if method != "GET":
                await send({"type": "http.response.start", "status": 405, "headers": METHOD_NOT_ALLOWED_HEADERS})
                await send({"type": "http.response.body", "body": METHOD_NOT_ALLOWED_BODY})
                return
            await send({"type": "http.response.start", "status": 200, "headers": HEALTH_HEADERS})
            await send({"type": "http.response.body", "body": HEALTH_BODY})
            return

        if path == "/webhooks/ingest":
            if method != "POST":
                await send({"type": "http.response.start", "status": 405, "headers": METHOD_NOT_ALLOWED_HEADERS})
                await send({"type": "http.response.body", "body": METHOD_NOT_ALLOWED_BODY})
                return

            # 1. Header extraction and validation
            key = None
            for h_name, h_val in scope["headers"]:
                if h_name == b"x-idempotency-key":
                    key = h_val
                    break

            if not key or not key.strip():
                body = b'{"detail":"Missing X-Idempotency-Key header"}'
                await send_json(send, 400, body)
                return

            # 2. Read request body within safe size limits
            body_chunks = []
            more_body = True
            total_size = 0
            while more_body:
                msg = await receive()
                chunk = msg.get("body", b"")
                if chunk:
                    body_chunks.append(chunk)
                    total_size += len(chunk)
                    if total_size > MAX_PAYLOAD_SIZE:
                        body = b'{"detail":"Payload too large"}'
                        await send_json(send, 413, body)
                        return
                more_body = msg.get("more_body", False)
            raw_bytes = body_chunks[0] if len(body_chunks) == 1 else b"".join(body_chunks)

            # 3. Parse JSON & Validate payload BEFORE reserving idempotency key
            try:
                data = pydantic_core.from_json(raw_bytes)
            except Exception:
                body = b'{"detail":"Invalid JSON payload"}'
                await send_json(send, 400, body)
                return

            if not isinstance(data, dict):
                body = b'{"detail":"JSON body must be an object"}'
                await send_json(send, 400, body)
                return

            event_id = data.get("event_id")
            tenant_id = data.get("tenant_id")
            content = data.get("content")

            if not event_id or not isinstance(event_id, str):
                body = b'{"detail":"Field event_id must be a non-empty string"}'
                await send_json(send, 422, body)
                return

            if not tenant_id or not isinstance(tenant_id, str):
                body = b'{"detail":"Field tenant_id must be a non-empty string"}'
                await send_json(send, 422, body)
                return

            if content is None or not isinstance(content, str):
                body = b'{"detail":"Field content must be a string"}'
                await send_json(send, 422, body)
                return

            # Yield to event loop to allow concurrent health probes to run with zero starvation
            await asyncio.sleep(0)

            # 4. In-memory fast dedup check with payload consistency validation
            key_str = key.decode("utf-8", errors="replace")
            if key in seen_idempotency_keys:
                cached_evt_id, cached_tenant_id, cached_content = seen_idempotency_keys[key]
                # Compare event_id, tenant_id, and content for key reuse
                if cached_evt_id != event_id or cached_tenant_id != tenant_id or cached_content != content:
                    body = b'{"detail":"Idempotency key reused with different payload"}'
                    await send_json(send, 409, body)
                    return
                await send({"type": "http.response.start", "status": 202, "headers": DUP_HEADERS})
                await send({"type": "http.response.body", "body": DUP_BODY})
                return

            if key in in_flight_keys:
                inf_op = in_flight_keys[key]
                if inf_op.event_id != event_id or inf_op.tenant_id != tenant_id or inf_op.raw_content != content:
                    body = b'{"detail":"Idempotency key reused with different payload"}'
                    await send_json(send, 409, body)
                    return
                await inf_op.event.wait()
                if inf_op.error is not None:
                    # Initial insert failed with an exception; do not acknowledge blindly
                    pass
                elif inf_op.status_result == "payload_mismatch":
                    body = b'{"detail":"Idempotency key reused with different payload"}'
                    await send_json(send, 409, body)
                    return
                elif inf_op.status_result in ("inserted", "duplicate"):
                    await send({"type": "http.response.start", "status": 202, "headers": DUP_HEADERS})
                    await send({"type": "http.response.body", "body": DUP_BODY})
                    return

            if event_id in seen_event_ids:
                # Query SQLite to inspect whether this is genuine duplicate or conflict
                status_result, existing_evt_id, existing_key = await insert_or_check_conflict(
                    event_id=event_id,
                    idempotency_key=key_str,
                    tenant_id=tenant_id,
                    raw_content=content,
                )
                if status_result == "payload_mismatch":
                    body = b'{"detail":"Idempotency key reused with different payload"}'
                    await send_json(send, 409, body)
                    return
                await send({"type": "http.response.start", "status": 202, "headers": DUP_HEADERS})
                await send({"type": "http.response.body", "body": DUP_BODY})
                return

            if event_id in in_flight_events:
                inf_op = in_flight_events[event_id]
                if inf_op.idempotency_key != key_str or inf_op.tenant_id != tenant_id or inf_op.raw_content != content:
                    body = b'{"detail":"Idempotency key reused with different payload"}'
                    await send_json(send, 409, body)
                    return
                await inf_op.event.wait()
                if inf_op.error is not None:
                    # Initial insert failed with an exception; do not acknowledge blindly
                    pass
                elif inf_op.status_result == "payload_mismatch":
                    body = b'{"detail":"Idempotency key reused with different payload"}'
                    await send_json(send, 409, body)
                    return
                elif inf_op.status_result in ("inserted", "duplicate"):
                    await send({"type": "http.response.start", "status": 202, "headers": DUP_HEADERS})
                    await send({"type": "http.response.body", "body": DUP_BODY})
                    return

            # 5. Persist event as 'pending' in SQLite BEFORE acknowledging
            my_op = InFlightOp(event_id, key_str, tenant_id, content)
            in_flight_keys[key] = my_op
            in_flight_events[event_id] = my_op
            try:
                status_result, existing_evt_id, existing_key = await insert_or_check_conflict(
                    event_id=event_id,
                    idempotency_key=key_str,
                    tenant_id=tenant_id,
                    raw_content=content,
                )
                my_op.status_result = status_result
                my_op.existing_evt_id = existing_evt_id
                my_op.existing_key = existing_key

                if status_result == "payload_mismatch":
                    # Key was previously used with different content
                    body = b'{"detail":"Idempotency key reused with different payload"}'
                    await send_json(send, 409, body)
                    return

                if status_result == "duplicate":
                    # Only cache what is actually persisted in SQLite
                    if existing_key:
                        seen_idempotency_keys[existing_key.encode("utf-8")] = (existing_evt_id or event_id, tenant_id, content)
                    if existing_evt_id:
                        seen_event_ids.add(existing_evt_id)
                    await send({"type": "http.response.start", "status": 202, "headers": DUP_HEADERS})
                    await send({"type": "http.response.body", "body": DUP_BODY})
                    return

                # 6. Newly inserted event: safe to reserve and signal worker
                seen_idempotency_keys[key] = (event_id, tenant_id, content)
                seen_event_ids.add(event_id)
                job_signal.release()

                await send({"type": "http.response.start", "status": 202, "headers": ACCEPTED_HEADERS})
                await send({"type": "http.response.body", "body": ACCEPTED_BODY})
                return
            except Exception as exc:
                my_op.error = exc
                raise
            finally:
                my_op.event.set()
                in_flight_keys.pop(key, None)
                in_flight_events.pop(event_id, None)

    await raw_fastapi_app(scope, receive, send)

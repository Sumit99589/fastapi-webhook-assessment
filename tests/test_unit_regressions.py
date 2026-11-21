import pytest
import asyncio
import httpx
from unittest.mock import patch
from app.database import (
    init_db,
    close_db,
    get_db,
    insert_or_check_conflict,
    claim_next_job,
    renew_lease,
    schedule_transient_retry,
    mark_permanent_failure,
    mark_delivered,
)
from app.main import app, raw_fastapi_app, lifespan


@pytest.fixture(autouse=True)
async def setup_test_db(tmp_path, monkeypatch):
    test_db = tmp_path / "test_regressions.db"
    monkeypatch.setenv("WEBHOOK_DB_PATH", str(test_db))
    await init_db(str(test_db))
    yield
    await close_db()


@pytest.mark.asyncio
async def test_idempotency_cache_does_not_poison_unpersisted_event_id():
    """
    Verifies that when Event B conflicts with an existing key,
    Event B's ID is not cached, and Event B can be submitted with a new key.
    """
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # 1. Event A arrives with K1
        r1 = await client.post(
            "/webhooks/ingest",
            json={"event_id": "evt_A", "tenant_id": "t1", "content": "content A"},
            headers={"X-Idempotency-Key": "K1"},
        )
        assert r1.status_code == 202

        # 2. Event B arrives with K1 (conflict on K1 due to different payload -> 409)
        r2 = await client.post(
            "/webhooks/ingest",
            json={"event_id": "evt_B", "tenant_id": "t1", "content": "content B"},
            headers={"X-Idempotency-Key": "K1"},
        )
        assert r2.status_code == 409

        # 3. Event B arrives with a genuinely new key K2 -> MUST BE ACCEPTED
        # (confirms evt_B was not poisoned by the failed attempt with K1)
        r3 = await client.post(
            "/webhooks/ingest",
            json={"event_id": "evt_B", "tenant_id": "t1", "content": "content B"},
            headers={"X-Idempotency-Key": "K2"},
        )
        assert r3.status_code == 202
        assert r3.json() == {"status": "accepted"}


@pytest.mark.asyncio
async def test_recovery_handles_more_than_100_events(tmp_path, monkeypatch):
    test_db = tmp_path / "test_regressions.db"
    total_events = 150

    for i in range(total_events):
        await insert_or_check_conflict(
            event_id=f"evt_bulk_{i:03d}",
            idempotency_key=f"key_bulk_{i:03d}",
            tenant_id="tenant_bulk",
            raw_content=f"Payload {i}",
            db_path=str(test_db),
        )

    claimed_events = set()
    while True:
        job = await claim_next_job(str(test_db))
        if job is None:
            break
        evt_id = job[0]
        assert evt_id not in claimed_events, f"Duplicate claim for {evt_id}"
        claimed_events.add(evt_id)
        await mark_delivered(evt_id, "clean", "fp", db_path=str(test_db))

    assert len(claimed_events) == total_events, f"Expected {total_events} claimed, got {len(claimed_events)}"


@pytest.mark.asyncio
async def test_permanent_400_is_not_scheduled_for_retry(tmp_path, monkeypatch):
    """
    Verifies that permanent errors (e.g. downstream 400 Bad Request) are marked
    as 'failed' immediately in SQLite and NEVER scheduled as 'retry_scheduled'.
    """
    test_db = tmp_path / "test_regressions.db"
    await insert_or_check_conflict(
        event_id="evt_perm_400",
        idempotency_key="key_perm_400",
        tenant_id="t1",
        raw_content="content",
        db_path=str(test_db),
    )

    job = await claim_next_job(str(test_db))
    assert job is not None
    # Simulate permanent downstream rejection
    await mark_permanent_failure(
        event_id="evt_perm_400",
        error_message="Client Error 400 Bad Request",
        attempts=1,
        db_path=str(test_db),
    )

    db = await get_db(str(test_db))
    async with db.execute("SELECT status, next_retry_at FROM events WHERE event_id = 'evt_perm_400'") as cur:
        row = await cur.fetchone()
        assert row == ("failed", None)

    # Subsequent claim must find no eligible jobs
    next_claim = await claim_next_job(str(test_db))
    assert next_claim is None


@pytest.mark.asyncio
async def test_lease_based_job_recovery_and_worker_ownership(tmp_path, monkeypatch):
    """
    Verifies that:
    1. Active processing jobs have worker ownership tokens (owner_id).
    2. Heartbeat (renew_lease) keeps active jobs alive.
    3. Startup recovery does NOT steal active jobs from running workers.
    4. Reclaimed jobs fence out stale workers from committing state.
    """
    test_db = tmp_path / "test_regressions.db"
    await insert_or_check_conflict(
        event_id="evt_active",
        idempotency_key="key_active",
        tenant_id="t1",
        raw_content="content",
        db_path=str(test_db),
    )

    # 1. Worker A claims job
    job = await claim_next_job(worker_id="worker-A", db_path=str(test_db))
    assert job is not None
    assert job[0] == "evt_active"
    assert job[5] == "worker-A"

    # 2. Worker A renews lease via heartbeat
    renewed = await renew_lease("evt_active", "worker-A", db_path=str(test_db))
    assert renewed is True

    # Worker B cannot renew Worker A's lease
    wrong_renew = await renew_lease("evt_active", "worker-B", db_path=str(test_db))
    assert wrong_renew is False

    # 3. Another worker starting up (calling init_db) must NOT steal active job
    await init_db(str(test_db))
    db = await get_db(str(test_db))
    async with db.execute("SELECT status, owner_id FROM events WHERE event_id = 'evt_active'") as cur:
        row = await cur.fetchone()
        assert row == ("processing", "worker-A")

    # 4. Simulate lease expiration (updated_at set to 125 seconds ago)
    await db.execute(
        "UPDATE events SET updated_at = datetime('now', '-125 seconds') WHERE event_id = 'evt_active'"
    )
    await db.commit()

    # Expired lease is reclaimed by Worker B
    reclaimed = await claim_next_job(worker_id="worker-B", db_path=str(test_db))
    assert reclaimed is not None
    assert reclaimed[0] == "evt_active"
    assert reclaimed[5] == "worker-B"

    # Stale Worker A attempting to mark delivered is fenced out!
    stale_deliver = await mark_delivered(
        event_id="evt_active",
        owner_id="worker-A",
        cleaned_text="stale",
        fingerprint="stale",
        db_path=str(test_db),
    )
    assert stale_deliver is False, "Stale worker must be fenced out from marking delivered"

    # Active Worker B delivers successfully
    active_deliver = await mark_delivered(
        event_id="evt_active",
        owner_id="worker-B",
        cleaned_text="delivered",
        fingerprint="valid",
        db_path=str(test_db),
    )
    assert active_deliver is True


@pytest.mark.asyncio
async def test_stale_worker_cannot_overwrite_retry_scheduled_or_failed_job(tmp_path, monkeypatch):
    """
    Verifies that if a job transitions to retry_scheduled (where owner_id is NULL)
    or failed, a stale worker with an old token or NULL owner fallback CANNOT
    change status back to delivered.
    """
    test_db = tmp_path / "test_regressions.db"
    await insert_or_check_conflict(
        event_id="evt_fencing",
        idempotency_key="key_fencing",
        tenant_id="t1",
        raw_content="content",
        db_path=str(test_db),
    )

    # Worker 1 claims job
    job = await claim_next_job(worker_id="token-1", db_path=str(test_db))
    assert job is not None
    assert job[5] == "token-1"

    # Worker 1 experiences transient error -> schedules retry, releasing owner_id (owner_id becomes NULL)
    retried = await schedule_transient_retry(
        event_id="evt_fencing",
        error_message="Downstream 503",
        attempts=1,
        owner_id="token-1",
        db_path=str(test_db),
    )
    assert retried is True

    db = await get_db(str(test_db))
    async with db.execute("SELECT status, owner_id FROM events WHERE event_id = 'evt_fencing'") as cur:
        row = await cur.fetchone()
        assert row[0] == "retry_scheduled"
        assert row[1] is None

    # Stale Worker 1 late completion attempting to mark delivered MUST FAIL
    # because row is no longer in 'processing' and owner_id does not match
    stale_delivered = await mark_delivered(
        event_id="evt_fencing",
        cleaned_text="late_clean",
        fingerprint="late_fp",
        owner_id="token-1",
        db_path=str(test_db),
    )
    assert stale_delivered is False, "Late completion must not mark retry_scheduled event as delivered"

    # Verify status is still retry_scheduled
    async with db.execute("SELECT status, owner_id FROM events WHERE event_id = 'evt_fencing'") as cur:
        row = await cur.fetchone()
        assert row[0] == "retry_scheduled"
        assert row[1] is None


@pytest.mark.asyncio
async def test_inflight_duplicate_does_not_acknowledge_if_initial_insert_fails():
    """
    Verifies Issue A fix: If Request 1 encounters an exception during database insertion,
    a concurrent Request 2 waiting in in_flight_keys does NOT return HTTP 202 duplicate acknowledgment
    for unpersisted state. Request 2 detects the error and safely performs durable persistence.
    """
    insert_call_count = 0
    original_insert = insert_or_check_conflict

    async def mock_insert(*args, **kwargs):
        nonlocal insert_call_count
        insert_call_count += 1
        if insert_call_count == 1:
            # Yield so Request 2 arrives and queues behind InFlightOp
            await asyncio.sleep(0.04)
            raise RuntimeError("Simulated SQLite failure on Request 1")
        return await original_insert(*args, **kwargs)

    payload = {"event_id": "evt_inflight_fail", "tenant_id": "t1", "content": "content"}
    headers = {"X-Idempotency-Key": "key_inflight_fail"}

    with patch("app.main.insert_or_check_conflict", side_effect=mock_insert):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            results = await asyncio.gather(
                client.post("/webhooks/ingest", json=payload, headers=headers),
                client.post("/webhooks/ingest", json=payload, headers=headers),
                return_exceptions=True,
            )

            # Request 1 must have failed with an exception
            exceptions = [r for r in results if isinstance(r, Exception)]
            assert len(exceptions) == 1
            assert "Simulated SQLite failure on Request 1" in str(exceptions[0])

            # Request 2 must NOT have returned a false duplicate 202
            duplicate_202s = [
                r for r in results
                if isinstance(r, httpx.Response) and r.status_code == 202 and r.json().get("duplicate") is True
            ]
            assert len(duplicate_202s) == 0, "In-flight waiter must not acknowledge duplicate when initial insert fails"

            # Request 2 should have safely persisted the event as newly accepted
            successes = [
                r for r in results
                if isinstance(r, httpx.Response) and r.status_code == 202 and r.json().get("status") == "accepted"
            ]
            assert len(successes) == 1

            db = await get_db()
            async with db.execute("SELECT status FROM events WHERE event_id = 'evt_inflight_fail'") as cur:
                row = await cur.fetchone()
                assert row is not None
                assert row[0] == "pending"

import pytest
import httpx
from app.main import app
from app.database import init_db, close_db, get_db


@pytest.fixture(autouse=True)
async def setup_test_db(tmp_path, monkeypatch):
    test_db = tmp_path / "test_validation.db"
    monkeypatch.setenv("WEBHOOK_DB_PATH", str(test_db))
    await init_db(str(test_db))
    yield
    await close_db()


@pytest.mark.asyncio
async def test_http_methods():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # POST to /health should return 405
        resp = await client.post("/health")
        assert resp.status_code == 405
        assert resp.json() == {"detail": "Method not allowed"}

        # GET to /webhooks/ingest should return 405
        resp = await client.get("/webhooks/ingest")
        assert resp.status_code == 405
        assert resp.json() == {"detail": "Method not allowed"}


@pytest.mark.asyncio
async def test_missing_idempotency_key_header():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/webhooks/ingest",
            json={"event_id": "e1", "tenant_id": "t1", "content": "hello"},
        )
        assert resp.status_code == 400
        assert resp.json() == {"detail": "Missing X-Idempotency-Key header"}
        assert int(resp.headers["content-length"]) == len(resp.content)


@pytest.mark.asyncio
async def test_invalid_json_does_not_poison_key():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        key = "poison_test_key_001"

        # 1. Send malformed JSON with key
        resp1 = await client.post(
            "/webhooks/ingest",
            content=b"{malformed json",
            headers={"X-Idempotency-Key": key, "Content-Type": "application/json"},
        )
        assert resp1.status_code == 400
        assert resp1.json() == {"detail": "Invalid JSON payload"}

        # 2. Re-send valid request with the SAME key - must NOT be treated as duplicate
        resp2 = await client.post(
            "/webhooks/ingest",
            json={"event_id": "evt_valid_001", "tenant_id": "t1", "content": "valid payload"},
            headers={"X-Idempotency-Key": key},
        )
        assert resp2.status_code == 202
        assert resp2.json() == {"status": "accepted"}


@pytest.mark.asyncio
async def test_missing_or_invalid_required_fields():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        key = "field_val_key"

        # Missing event_id
        resp = await client.post(
            "/webhooks/ingest",
            json={"tenant_id": "t1", "content": "hello"},
            headers={"X-Idempotency-Key": f"{key}_1"},
        )
        assert resp.status_code == 422
        assert "event_id" in resp.json()["detail"]

        # Empty event_id
        resp = await client.post(
            "/webhooks/ingest",
            json={"event_id": "", "tenant_id": "t1", "content": "hello"},
            headers={"X-Idempotency-Key": f"{key}_2"},
        )
        assert resp.status_code == 422

        # Missing tenant_id
        resp = await client.post(
            "/webhooks/ingest",
            json={"event_id": "e1", "content": "hello"},
            headers={"X-Idempotency-Key": f"{key}_3"},
        )
        assert resp.status_code == 422
        assert "tenant_id" in resp.json()["detail"]

        # Missing content
        resp = await client.post(
            "/webhooks/ingest",
            json={"event_id": "e1", "tenant_id": "t1"},
            headers={"X-Idempotency-Key": f"{key}_4"},
        )
        assert resp.status_code == 422
        assert "content" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_idempotency_key_payload_mismatch_returns_409():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        key = "payload_mismatch_key_001"

        # 1. First valid ingestion
        resp1 = await client.post(
            "/webhooks/ingest",
            json={"event_id": "evt_match_1", "tenant_id": "t1", "content": "Original Content"},
            headers={"X-Idempotency-Key": key},
        )
        assert resp1.status_code == 202
        assert resp1.json() == {"status": "accepted"}

        # 2. Duplicate with identical content -> 202 duplicate
        resp2 = await client.post(
            "/webhooks/ingest",
            json={"event_id": "evt_match_1", "tenant_id": "t1", "content": "Original Content"},
            headers={"X-Idempotency-Key": key},
        )
        assert resp2.status_code == 202
        assert resp2.json() == {"status": "accepted", "duplicate": True}

        # 3. Same key with DIFFERENT content -> 409 Conflict
        resp3 = await client.post(
            "/webhooks/ingest",
            json={"event_id": "evt_match_1", "tenant_id": "t1", "content": "Tampered Content"},
            headers={"X-Idempotency-Key": key},
        )
        assert resp3.status_code == 409
        assert "Idempotency key reused with different payload" in resp3.json()["detail"]

        # 4. Same key with DIFFERENT event_id (evt_B vs evt_match_1) but same content -> 409 Conflict
        resp4 = await client.post(
            "/webhooks/ingest",
            json={"event_id": "evt_B_different", "tenant_id": "t1", "content": "Original Content"},
            headers={"X-Idempotency-Key": key},
        )
        assert resp4.status_code == 409
        assert "Idempotency key reused with different payload" in resp4.json()["detail"]

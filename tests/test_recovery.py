import pytest
import asyncio
from unittest.mock import patch
from app.database import init_db, close_db, insert_or_check_conflict, get_db
from app.main import raw_fastapi_app, lifespan


@pytest.mark.asyncio
async def test_recovery_of_pending_events_on_startup(tmp_path, monkeypatch):
    test_db = tmp_path / "test_recovery.db"
    monkeypatch.setenv("WEBHOOK_DB_PATH", str(test_db))
    await init_db(str(test_db))

    # Simulate an event inserted into DB before service crashed
    status, _, _ = await insert_or_check_conflict(
        event_id="evt_recovered_1",
        idempotency_key="key_rec_1",
        tenant_id="tenant_rec",
        raw_content="SSN 123-45-6789 test data",
        db_path=str(test_db),
    )
    assert status == "inserted"

    # Mock forward_event so downstream network isn't required for recovery test
    with patch("app.main.forward_event", return_value={"status": "delivered"}):
        async with lifespan(raw_fastapi_app):
            # Wait for background worker to consume recovered pending item
            await asyncio.sleep(0.5)

    db = await get_db(str(test_db))
    async with db.execute("SELECT status, cleaned_text FROM events WHERE event_id = 'evt_recovered_1'") as cur:
        row = await cur.fetchone()
        assert row is not None
        status, cleaned_text = row
        assert status == "delivered"
        assert cleaned_text is not None
        assert "123-45-6789" not in cleaned_text
        assert "[REDACTED]" in cleaned_text

    await close_db()

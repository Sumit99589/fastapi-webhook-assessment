import pytest
import httpx
from unittest.mock import AsyncMock, patch
from app.downstream import forward_event


@pytest.mark.asyncio
async def test_downstream_400_is_not_retried():
    call_count = 0

    def mock_handler(request: httpx.Request):
        nonlocal call_count
        call_count += 1
        return httpx.Response(400, json={"detail": "Bad Request"})

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await forward_event(
                event_id="evt_400",
                cleaned_text="test text",
                fingerprint="abc",
                client=client,
                max_retries=6,
                base_delay=0.01,
            )

    # Must fail immediately on the 1st attempt, NOT retried 6 times!
    assert call_count == 1, f"Expected 1 call for 400 Bad Request, got {call_count}"


@pytest.mark.asyncio
async def test_downstream_503_is_retried_and_succeeds():
    call_count = 0

    def mock_handler(request: httpx.Request):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            return httpx.Response(503, json={"detail": "Service Unavailable"})
        return httpx.Response(200, json={"status": "delivered"})

    transport = httpx.MockTransport(mock_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await forward_event(
            event_id="evt_503",
            cleaned_text="test text",
            fingerprint="abc",
            client=client,
            max_retries=6,
            base_delay=0.01,
        )

    assert call_count == 3
    assert result == {"status": "delivered"}

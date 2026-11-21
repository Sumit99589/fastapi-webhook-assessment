import asyncio
import logging
import os
import random
import httpx

logger = logging.getLogger(__name__)

DOWNSTREAM_URL = os.getenv("DOWNSTREAM_URL", "http://127.0.0.1:9001/events")

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


async def forward_event(
    event_id: str,
    cleaned_text: str,
    fingerprint: str,
    client: httpx.AsyncClient | None = None,
    idempotency_key: str | None = None,
    max_retries: int = 6,
    base_delay: float = 0.05,
) -> dict:
    """
    Forwards a processed webhook event to the downstream consumer API.
    Retries only on transient failures (503, 502, 504, 429, and transport errors)
    using exponential backoff with jitter. Non-retryable errors (e.g. 400, 404)
    fail immediately without retry.
    """
    url = os.getenv("DOWNSTREAM_URL", DOWNSTREAM_URL)
    payload = {
        "event_id": event_id,
        "text": cleaned_text,
        "fingerprint": fingerprint,
    }
    headers = {}
    if idempotency_key:
        headers["X-Idempotency-Key"] = idempotency_key

    should_close = False
    if client is None:
        client = httpx.AsyncClient(timeout=10.0)
        should_close = True

    try:
        last_exception = None
        for attempt in range(1, max_retries + 1):
            try:
                response = await client.post(url, json=payload, headers=headers)
                if response.status_code in RETRYABLE_STATUS_CODES:
                    if attempt < max_retries:
                        jitter = random.uniform(0.005, 0.03)
                        delay = (base_delay * (1.5 ** (attempt - 1))) + jitter
                        await asyncio.sleep(delay)
                        continue
                    response.raise_for_status()

                # Non-retryable 4xx errors will raise HTTPStatusError immediately
                response.raise_for_status()
                return response.json()
            except httpx.TransportError as exc:
                last_exception = exc
                if attempt < max_retries:
                    jitter = random.uniform(0.005, 0.03)
                    delay = (base_delay * (1.5 ** (attempt - 1))) + jitter
                    await asyncio.sleep(delay)
                    continue
                raise last_exception
            except httpx.HTTPStatusError as exc:
                # If it's a non-retryable 4xx error (e.g. 400 Bad Request), fail immediately
                if exc.response.status_code not in RETRYABLE_STATUS_CODES:
                    raise exc
                last_exception = exc
                if attempt < max_retries:
                    jitter = random.uniform(0.005, 0.03)
                    delay = (base_delay * (1.5 ** (attempt - 1))) + jitter
                    await asyncio.sleep(delay)
                    continue
                raise last_exception

        raise RuntimeError(f"Failed to deliver event {event_id} after {max_retries} attempts")
    finally:
        if should_close:
            await client.aclose()

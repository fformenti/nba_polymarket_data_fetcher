from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from tenacity import RetryError

from polymarket.client import AsyncRateLimiter, get

# ── AsyncRateLimiter ──────────────────────────────────────────────────────────


async def test_acquire_decrements_tokens():
    limiter = AsyncRateLimiter(rate=100)
    await limiter.acquire()
    assert limiter.tokens == 99.0


async def test_acquire_sleeps_when_depleted():
    limiter = AsyncRateLimiter(rate=100)
    # Prime the limiter so last_refill is set, then drain tokens.
    await limiter.acquire()
    limiter.tokens = 0.0

    with patch("polymarket.client.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await limiter.acquire()

    mock_sleep.assert_awaited_once()
    sleep_duration = mock_sleep.await_args[0][0]
    assert sleep_duration > 0


async def test_acquire_refills_on_elapsed_time():
    limiter = AsyncRateLimiter(rate=100)
    await limiter.acquire()  # initialize last_refill
    limiter.tokens = 0.0
    assert limiter.last_refill is not None
    limiter.last_refill -= 30  # simulate 30s elapsed → refill ~50 tokens

    with patch("polymarket.client.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await limiter.acquire()

    mock_sleep.assert_not_awaited()
    assert limiter.tokens > 0


# ── get() ─────────────────────────────────────────────────────────────────────


def _mock_response(data: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()
    return resp


class _FakeAsyncClient:
    def __init__(self, resp) -> None:
        self._resp = resp
        self.calls: list[tuple[str, dict | None]] = []

    async def get(self, url: str, params: dict | None = None):
        self.calls.append((url, params))
        return self._resp

    async def aclose(self) -> None:
        pass


async def test_get_returns_json():
    expected = {"key": "value"}
    fake = _FakeAsyncClient(_mock_response(expected))

    with patch("polymarket.client._limiter.acquire", new_callable=AsyncMock), \
         patch("polymarket.client._get_client", return_value=fake):
        result = await get("https://example.com/api")

    assert result == expected
    assert fake.calls == [("https://example.com/api", None)]


async def test_get_passes_params():
    params = {"limit": 10, "offset": 0}
    fake = _FakeAsyncClient(_mock_response({"ok": True}))

    with patch("polymarket.client._limiter.acquire", new_callable=AsyncMock), \
         patch("polymarket.client._get_client", return_value=fake):
        await get("https://example.com/api", params=params)

    assert fake.calls[0][1] == params


async def test_get_raises_on_non_200(monkeypatch):
    """Non-200 response raises after retries are exhausted."""
    import asyncio as _asyncio

    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "404", request=MagicMock(), response=MagicMock()
    )
    fake = _FakeAsyncClient(mock_resp)

    async def _instant_sleep(_s):
        return None

    monkeypatch.setattr(_asyncio, "sleep", _instant_sleep)
    with patch("polymarket.client._limiter.acquire", new_callable=AsyncMock), \
         patch("polymarket.client._get_client", return_value=fake), pytest.raises(RetryError):
        await get("https://example.com/api")

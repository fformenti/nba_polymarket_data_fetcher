from __future__ import annotations

import asyncio

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

log = structlog.get_logger()

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"


class AsyncRateLimiter:
    """Token bucket: 100 req/min, safe under asyncio.gather."""

    def __init__(self, rate: int = 100):
        self.rate = rate
        self.tokens: float = float(rate)
        self.last_refill: float | None = None
        self._lock: asyncio.Lock | None = None

    async def acquire(self) -> None:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            if self.last_refill is None:
                self.last_refill = now
            elapsed = now - self.last_refill
            self.tokens = min(float(self.rate), self.tokens + elapsed * (self.rate / 60))
            self.last_refill = now
            if self.tokens < 1:
                sleep_time = (1 - self.tokens) * (60 / self.rate)
                await asyncio.sleep(sleep_time)
                self.tokens = 0.0
                self.last_refill = loop.time()
            else:
                self.tokens -= 1


_limiter = AsyncRateLimiter()
_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=30)
    return _client


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
async def get(url: str, params: dict | None = None) -> dict:
    await _limiter.acquire()
    log.info("api.request", url=url, params=params)
    client = _get_client()
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


async def aclose() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None

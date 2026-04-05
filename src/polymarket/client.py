import time

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

log = structlog.get_logger()

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"


class RateLimiter:
    """Simple token bucket: 100 req/min."""

    def __init__(self, rate: int = 100):
        self.rate = rate
        self.tokens = rate
        self.last_refill = time.monotonic()

    def acquire(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / 60))
        self.last_refill = now
        if self.tokens < 1:
            sleep_time = (1 - self.tokens) * (60 / self.rate)
            time.sleep(sleep_time)
        self.tokens -= 1


_limiter = RateLimiter()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def get(url: str, params: dict | None = None) -> dict:
    _limiter.acquire()
    log.info("api.request", url=url, params=params)
    resp = httpx.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

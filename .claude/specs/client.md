# Spec: HTTP Client (`src/polymarket/client.py`)

## Purpose
Single, shared async HTTP layer for all API calls. Owns rate limiting and retry logic so no other module needs to think about either.

## Exports

| Symbol | Type | Description |
|--------|------|-------------|
| `GAMMA_BASE` | `str` | `"https://gamma-api.polymarket.com"` |
| `CLOB_BASE` | `str` | `"https://clob.polymarket.com"` |
| `get` | `async def` | Primary fetch function |

## `RateLimiter`

Token bucket implementation.

```python
class RateLimiter:
    def __init__(self, rate: float = 100 / 60):  # tokens per second
        ...
    async def acquire(self) -> None: ...
```

- Capacity: 100 tokens (burst up to 100 consecutive requests)
- Refill rate: 1.667 tokens/second (100/min)
- `acquire()` blocks with `asyncio.sleep()` until a token is available

## `get(url, params=None) → dict`

```python
async def get(url: str, params: dict | None = None) -> dict: ...
```

### Behavior
1. Call `await _rate_limiter.acquire()`
2. Make `httpx.AsyncClient().get(url, params=params, timeout=10.0)`
3. On non-200: log `{url, status_code, body[:200]}` at WARNING level; raise `httpx.HTTPStatusError` so tenacity can retry
4. On success: `response.raise_for_status()` then `return response.json()`
5. Return: parsed JSON as `dict` (caller is responsible for Pydantic parsing)

### Retry (via tenacity)
- 3 total attempts
- `wait_exponential(multiplier=1, min=1, max=10)` seconds between retries
- Retry on: `httpx.HTTPStatusError`, `httpx.RequestError`
- After 3 failures: re-raise the exception (do not swallow)

### Logging (structlog)
- `log.info("fetch.start", url=url)` before the request
- `log.info("fetch.ok", url=url, status=200)` on success
- `log.warning("fetch.error", url=url, status=status_code)` on non-200

## Module-level singleton

```python
_rate_limiter = RateLimiter()
```

Shared across all calls in the process. Not thread-safe (not needed — we use asyncio).

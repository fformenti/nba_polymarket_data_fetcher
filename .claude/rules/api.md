# Polymarket API Rules

## API Layers & Their Purpose

| Layer | Base URL | Use For | Do NOT Use For |
|-------|----------|---------|----------------|
| Gamma API | `https://gamma-api.polymarket.com` | Market discovery, slugs, metadata | Price data |
| CLOB API | `https://clob.polymarket.com` | Price history, order books, midpoints | Market discovery |
| Data API | `https://data-api.polymarket.com` | Open interest, analytics | (not yet in scope) |

## Rate Limits (enforce in client.py)

| Endpoint Group | Limit | Strategy |
|----------------|-------|----------|
| Gamma `/events` | 500 req / 10s | Batch discovery |
| Gamma `/markets` | 300 req / 10s | Per-page cursor |
| CLOB `/prices-history` | 1000 req / 10s | One call per token |
| Data `/trades` | 200 req / 10s | Not yet implemented |

Our global cap: **100 req/min** (token bucket in `client.py`). This stays below all per-endpoint limits.

## Authentication
- Read-only endpoints require **no authentication**. Do not add auth headers to GET requests.
- L1/L2 auth (private keys, HMAC) is **out of scope** — this project never places orders.

## Error Handling
- On non-200 response: log the status code, URL, and first 200 chars of the body. Do not raise — let tenacity retry.
- On HTTP 429 (rate limit): tenacity will back off. Do not add custom 429 handling.
- On HTTP 503 (Cloudflare throttle): same — tenacity handles it.

## NBA Market Identification
- Filter markets by tag `"NBA"` on the Gamma API (`/markets?tag=nba` or similar).
- Moneyline market: `question` field contains no modifiers like "Spread", "Total", "Halftime".
- YES token: `clobTokenIds[0]` (index 0 = "Yes" = the named team wins).
- NO token: `clobTokenIds[1]` (index 1 = "No").

## Price History Parameters
- `fidelity=60` for historical (hourly resolution, manageable payload).
- `fidelity=1` for live/recent games (one-minute resolution).
- `interval=max` on first fetch (full history). `startTs={last_ts}` on incremental fetches.
- Price `p` is a float in [0, 1] — directly represents win probability. No conversion needed.

## Postponed / Cancelled Games
- A flat-line at `p=0.50` in the price history signals a cancelled market (resolved 50/50).
- Flag these records with a `is_cancelled=True` field before writing to Parquet.

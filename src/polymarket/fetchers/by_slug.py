"""Fetch Polymarket market data keyed by game slug.

The Gamma API supports `?slug=<game_slug>` which returns exactly one market.
This avoids the expensive tag-based scan used by the legacy pipeline and lets
us parallelise across slugs with an asyncio semaphore.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

import structlog

from polymarket.client import CLOB_BASE, GAMMA_BASE, get
from polymarket.fetchers.price_history import fetch_price_history
from polymarket.fetchers.trade_history import fetch_price_history_from_trades
from polymarket.lookup import _date_from_slug
from polymarket.models import (
    GammaMarket,
    PricePoint,
    SlugFetchResult,
    TokenBundle,
    detect_cancelled,
)
from polymarket.storage.state import append_dead_letter

log = structlog.get_logger()

_OUTCOMES = ("No", "Yes")  # index 0 = away team, index 1 = home team


async def _fetch_midpoint(token_id: str) -> float | None:
    try:
        data = await get(f"{CLOB_BASE}/midpoint", params={"token_id": token_id})
        return float(data["mid"])
    except Exception as e:
        log.warning("by_slug.midpoint_error", token_id=token_id, error=str(e))
        return None


async def _fetch_history(token_id: str) -> list[PricePoint]:
    try:
        return await fetch_price_history(token_id)
    except Exception as e:
        log.warning("by_slug.price_history_error", token_id=token_id, error=str(e))
        return []


async def _build_token_bundle_open(token_id: str, outcome: str) -> TokenBundle:
    midpoint_task = asyncio.create_task(_fetch_midpoint(token_id))
    history_task = asyncio.create_task(_fetch_history(token_id))
    midpoint, history = await asyncio.gather(midpoint_task, history_task)
    return TokenBundle(
        token_id=token_id,
        outcome=outcome,
        midpoint=midpoint,
        history=history,
        is_cancelled=detect_cancelled(history),
    )


def _build_token_bundle_closed(
    token_id: str, outcome: str, history: list[PricePoint]
) -> TokenBundle:
    # Closed markets have no orderbook, so /midpoint would 500. Leave it None.
    return TokenBundle(
        token_id=token_id,
        outcome=outcome,
        midpoint=None,
        history=history,
        is_cancelled=detect_cancelled(history),
    )


async def fetch_by_slug(game_slug: str) -> SlugFetchResult | None:
    """Fetch metadata + snapshot + price history for one game slug.

    Returns None if the slug is missing, validation fails, or the response is
    unrecoverable. Errors are logged and appended to the dead-letter file.
    """
    log.info("by_slug.fetch_start", slug=game_slug)
    try:
        data = await get(f"{GAMMA_BASE}/events", params={"slug": game_slug})
    except Exception as e:
        log.error("by_slug.gamma_error", slug=game_slug, error=str(e))
        append_dead_letter(
            token_id="", error=str(e), context={"slug": game_slug, "stage": "gamma"}
        )
        return None

    event = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else None
    if not event:
        log.warning("by_slug.not_found", slug=game_slug)
        append_dead_letter(
            token_id="", error="event_not_found", context={"slug": game_slug, "stage": "gamma"}
        )
        return None

    event_markets = event.get("markets") or []
    raw = next((m for m in event_markets if m.get("slug") == game_slug), None)
    if not raw:
        log.warning(
            "by_slug.no_moneyline_market",
            slug=game_slug,
            event_id=event.get("id"),
            child_count=len(event_markets),
        )
        append_dead_letter(
            token_id="",
            error="no_moneyline_market",
            context={"slug": game_slug, "stage": "gamma", "event_id": event.get("id")},
        )
        return None

    try:
        market = GammaMarket.model_validate(raw)
    except Exception as e:
        log.warning("by_slug.validation_error", slug=game_slug, error=str(e))
        append_dead_letter(
            token_id="", error=str(e), context={"slug": game_slug, "stage": "validate"}
        )
        return None

    if not market.clob_token_ids:
        log.warning("by_slug.no_tokens", slug=game_slug, market_id=market.id)

    tokens: list[TokenBundle] = []
    if market.clob_token_ids:
        if market.closed:
            trade_buckets = await fetch_price_history_from_trades(market.condition_id)
            tokens = [
                _build_token_bundle_closed(
                    tid,
                    _OUTCOMES[i] if i < len(_OUTCOMES) else f"Outcome{i}",
                    trade_buckets.get(tid, []),
                )
                for i, tid in enumerate(market.clob_token_ids)
            ]
        else:
            token_tasks = [
                _build_token_bundle_open(
                    tid, _OUTCOMES[i] if i < len(_OUTCOMES) else f"Outcome{i}"
                )
                for i, tid in enumerate(market.clob_token_ids)
            ]
            tokens = list(await asyncio.gather(*token_tasks))

    game_date_val = _infer_game_date(market, game_slug)

    result = SlugFetchResult(
        slug=game_slug,
        game_date=game_date_val,
        fetched_at=datetime.now(UTC),
        market=market,
        tokens=list(tokens),
    )
    log.info(
        "by_slug.fetch_done",
        slug=game_slug,
        token_count=len(tokens),
        history_points=sum(len(t.history) for t in tokens),
    )
    return result


def _infer_game_date(market: GammaMarket, slug: str) -> date:
    parsed = _date_from_slug(slug)
    if parsed is not None:
        return parsed
    if market.end_date_iso:
        try:
            return date.fromisoformat(market.end_date_iso[:10])
        except ValueError:
            pass
    return date.today()


async def fetch_games_batch(
    game_slugs: list[str],
    *,
    concurrency: int = 8,
) -> list[SlugFetchResult]:
    """Fetch many slugs in parallel, capped at `concurrency` in-flight slugs."""
    sem = asyncio.Semaphore(concurrency)
    log.info("by_slug.batch_start", count=len(game_slugs), concurrency=concurrency)

    async def _one(slug: str) -> SlugFetchResult | None:
        async with sem:
            return await fetch_by_slug(slug)

    raw_results = await asyncio.gather(*(_one(s) for s in game_slugs), return_exceptions=True)
    results: list[SlugFetchResult] = []
    for slug, item in zip(game_slugs, raw_results, strict=True):
        if isinstance(item, Exception):
            log.error("by_slug.batch_exception", slug=slug, error=str(item))
            append_dead_letter(
                token_id="", error=str(item), context={"slug": slug, "stage": "batch"}
            )
            continue
        if item is None:
            continue
        results.append(item)

    log.info("by_slug.batch_done", fetched=len(results), total=len(game_slugs))
    return results

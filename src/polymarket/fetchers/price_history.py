from __future__ import annotations

from collections.abc import Callable

import structlog

from polymarket.client import CLOB_BASE, get
from polymarket.models import PricePoint
from polymarket.storage.state import load_state, save_state

log = structlog.get_logger()

STATE_KEY = "price_history"


async def fetch_price_history(
    token_id: str,
    commit_checkpoint: Callable[[int], None] | None = None,
) -> list[PricePoint]:
    state = load_state(STATE_KEY)
    last_ts: int | None = state.get(token_id)

    params: dict = {"market": token_id, "fidelity": 60}
    if last_ts is not None:
        params["startTs"] = last_ts
    else:
        params["interval"] = "max"

    log.info("price_history.fetch_start", token_id=token_id, start_ts=last_ts)
    data = await get(f"{CLOB_BASE}/prices-history", params=params)

    points = []
    for item in data.get("history", []):
        try:
            points.append(PricePoint(t=item["t"], p=item["p"]))
        except Exception as e:
            log.warning(
                "price_history.validation_error", token_id=token_id, error=str(e), item=item
            )

    if points:
        max_ts = max(pt.t for pt in points)
        if commit_checkpoint is not None:
            commit_checkpoint(max_ts)
        else:
            state[token_id] = max_ts
            save_state(STATE_KEY, state)
        log.info(
            "price_history.fetch_complete", token_id=token_id, count=len(points), last_ts=max_ts
        )
    else:
        log.info("price_history.fetch_complete", token_id=token_id, count=0)

    return points

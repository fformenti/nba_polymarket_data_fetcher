import structlog

from polymarket.client import CLOB_BASE, get
from polymarket.models import PricePoint

log = structlog.get_logger()


def fetch_price_history(token_id: str, start_ts: int | None = None) -> list[PricePoint]:
    params = {"market": token_id, "fidelity": 60}  # 1-hour resolution
    if start_ts:
        params["startTs"] = start_ts
    else:
        params["interval"] = "max"  # Full history for first fetch

    data = get(f"{CLOB_BASE}/prices-history", params=params)
    points = []
    for item in data.get("history", []):
        try:
            points.append(PricePoint(t=item["t"], p=item["p"]))
        except Exception as e:
            log.warning("price_history.validation_error", token_id=token_id, error=str(e))
    return points

import json
from pathlib import Path

import structlog

from polymarket.client import GAMMA_BASE, get
from polymarket.models import GammaMarket

log = structlog.get_logger()
STATE_FILE = Path("data/state/markets.json")
PAGE_SIZE = 100


def load_state() -> int:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text()).get("offset", 0)
    return 0


def save_state(offset: int):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps({"offset": offset}))


def fetch_all_markets() -> list[GammaMarket]:
    offset = load_state()
    all_markets = []

    while True:
        data = get(
            f"{GAMMA_BASE}/markets",
            params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "order": "id",
                "ascending": True,
            },
        )
        if not data:
            break

        valid = []
        for item in data:
            try:
                valid.append(GammaMarket.model_validate(item))
            except Exception as e:
                log.warning("market.validation_error", error=str(e), item_id=item.get("id"))

        all_markets.extend(valid)
        offset += len(data)
        save_state(offset)
        log.info("markets.page_fetched", count=len(valid), offset=offset)

        if len(data) < PAGE_SIZE:
            break  # Last page

    return all_markets

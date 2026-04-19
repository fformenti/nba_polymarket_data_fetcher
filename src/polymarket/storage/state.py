from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

import structlog as _structlog

_STATE_DIR = Path("data/state")


def load_state(key: str) -> dict[str, Any]:
    path = _STATE_DIR / f"{key}.json"
    if path.exists():
        return json.loads(path.read_text())
    return {}


def save_state(key: str, state: dict[str, Any]) -> None:
    _STATE_DIR.mkdir(parents=True, exist_ok=True)
    (_STATE_DIR / f"{key}.json").write_text(json.dumps(state))


_DEAD_LETTER_PATH = _STATE_DIR / "dead_letter.jsonl"


def append_dead_letter(
    token_id: str,
    error: str,
    context: dict[str, Any],
) -> None:
    _STATE_DIR.mkdir(parents=True, exist_ok=True)
    entry: dict[str, Any] = {
        "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        "token_id": token_id,
        "error": error,
        "context": context,
    }
    try:
        with _DEAD_LETTER_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except OSError as exc:
        _structlog.get_logger().warning(
            "dead_letter.write_failed", token_id=token_id, exc=str(exc)
        )

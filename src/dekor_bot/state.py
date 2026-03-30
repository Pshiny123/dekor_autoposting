from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _dt_from_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class BotState:
    index: int
    last_posted_at: datetime | None

    @staticmethod
    def initial() -> "BotState":
        return BotState(index=0, last_posted_at=None)

    @staticmethod
    def load(path: Path) -> "BotState":
        if not path.exists():
            return BotState.initial()
        data = json.loads(path.read_text(encoding="utf-8"))
        index = int(data.get("index", 0))
        last = data.get("last_posted_at")
        last_dt = _dt_from_iso(last) if isinstance(last, str) and last else None
        return BotState(index=index, last_posted_at=last_dt)

    def save(self, path: Path) -> None:
        payload: dict[str, Any] = {
            "index": int(self.index),
            "last_posted_at": _dt_to_iso(self.last_posted_at) if self.last_posted_at else None,
            "saved_at": _dt_to_iso(_utc_now()),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


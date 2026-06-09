from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

_MAX_ENTRIES = 500
_lock = Lock()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log_path() -> Path:
    raw = (os.getenv("RUN_LOG_PATH") or "data/run_log.json").strip()
    return Path(raw)


def _redact_secrets(text: str) -> str:
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if token and token in text:
        text = text.replace(token, "***")
    return text


def _sanitize_entry(entry: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in entry.items():
        if isinstance(value, str):
            out[key] = _redact_secrets(value)
        else:
            out[key] = value
    return out


def append_run_log(entry: dict[str, Any]) -> None:
    """
    Дописывает запись о запуске в JSON-файл (массив runs).
    Каждый запуск бота / планировщика оставляет след: запланировано, отправлено, ошибка.
    """
    path = _log_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    record = _sanitize_entry(
        {
            "logged_at": _utc_now_iso(),
            **entry,
        }
    )

    with _lock:
        runs: list[dict[str, Any]] = []
        if path.is_file():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict) and isinstance(data.get("runs"), list):
                    runs = data["runs"]
                elif isinstance(data, list):
                    runs = data
            except (OSError, json.JSONDecodeError):
                runs = []

        runs.append(record)
        if len(runs) > _MAX_ENTRIES:
            runs = runs[-_MAX_ENTRIES :]

        payload = {"runs": runs}
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

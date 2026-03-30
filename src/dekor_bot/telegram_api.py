from __future__ import annotations

import json
import mimetypes
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import requests


ParseMode = Literal["HTML", "MarkdownV2"]


@dataclass(frozen=True)
class TelegramClient:
    token: str
    timeout_s: int = 60

    @property
    def base_url(self) -> str:
        return f"https://api.telegram.org/bot{self.token}"

    def _post(self, method: str, data: dict[str, Any] | None = None, files: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}/{method}"
        resp = requests.post(url, data=data or {}, files=files, timeout=self.timeout_s)
        try:
            payload = resp.json()
        except Exception:
            resp.raise_for_status()
            raise
        if not payload.get("ok"):
            raise RuntimeError(f"Telegram API error ({method}): {payload}")
        return payload

    def send_message(
        self,
        chat_id: str | int,
        text: str,
        parse_mode: ParseMode = "HTML",
        *,
        disable_web_page_preview: bool = False,
    ) -> dict[str, Any]:
        return self._post(
            "sendMessage",
            data={
                "chat_id": str(chat_id),
                "text": text,
                "parse_mode": parse_mode,
                **({"disable_web_page_preview": "true"} if disable_web_page_preview else {}),
            },
        )

    def send_photo(self, chat_id: str | int, photo: str, caption: str | None = None, parse_mode: ParseMode = "HTML") -> dict[str, Any]:
        if _is_url(photo):
            return self._post(
                "sendPhoto",
                data={
                    "chat_id": str(chat_id),
                    "photo": photo,
                    **({"caption": caption, "parse_mode": parse_mode} if caption else {}),
                },
            )
        p = Path(photo)
        with p.open("rb") as f:
            return self._post(
                "sendPhoto",
                data={
                    "chat_id": str(chat_id),
                    **({"caption": caption, "parse_mode": parse_mode} if caption else {}),
                },
                files={"photo": (p.name, f, mimetypes.guess_type(p.name)[0] or "application/octet-stream")},
            )

    def send_video(self, chat_id: str | int, video: str, caption: str | None = None, parse_mode: ParseMode = "HTML") -> dict[str, Any]:
        if _is_url(video):
            return self._post(
                "sendVideo",
                data={
                    "chat_id": str(chat_id),
                    "video": video,
                    **({"caption": caption, "parse_mode": parse_mode} if caption else {}),
                },
            )
        p = Path(video)
        with p.open("rb") as f:
            return self._post(
                "sendVideo",
                data={
                    "chat_id": str(chat_id),
                    **({"caption": caption, "parse_mode": parse_mode} if caption else {}),
                },
                files={"video": (p.name, f, mimetypes.guess_type(p.name)[0] or "application/octet-stream")},
            )

    def send_media_group(
        self,
        chat_id: str | int,
        media: list[dict[str, Any]],
    ) -> dict[str, Any]:
        # media: [{type, media, caption?, parse_mode?}, ...]
        return self._post(
            "sendMediaGroup",
            data={"chat_id": str(chat_id), "media": json.dumps(media, ensure_ascii=False)},
        )


def _is_url(s: str) -> bool:
    s = s.strip().lower()
    return s.startswith("http://") or s.startswith("https://")


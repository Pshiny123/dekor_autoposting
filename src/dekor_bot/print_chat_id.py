from __future__ import annotations

import os
from dotenv import load_dotenv

from .telegram_api import TelegramClient


def main() -> None:
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("Не задан TELEGRAM_BOT_TOKEN (создайте .env).")

    tg = TelegramClient(token=token)
    payload = tg._post("getUpdates", data={"timeout": "0"})  # noqa: SLF001
    result = payload.get("result", [])
    if not result:
        print("Нет апдейтов. Напишите боту любое сообщение и запустите снова.")
        return

    # Выведем последние 10 апдейтов, чтобы легко найти нужный чат/канал.
    for upd in result[-10:]:
        msg = upd.get("message") or upd.get("channel_post") or upd.get("edited_message") or upd.get("edited_channel_post")
        if not msg:
            continue
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        title = chat.get("title") or ""
        username = chat.get("username") or ""
        chat_type = chat.get("type") or ""
        print(f"chat_id={chat_id} type={chat_type} title={title!r} username=@{username}" if username else f"chat_id={chat_id} type={chat_type} title={title!r}")


if __name__ == "__main__":
    main()


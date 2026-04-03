from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from .excel_meta import (
    has_meta_sheets,
    read_queue_post_ids,
    read_settings_chat_id,
    read_state,
    write_state,
)
from .excel_posts import index_posts_by_id, load_posts
from .state import BotState
from .telegram_api import TelegramClient
from .main import _send_post, setup_logging

logger = logging.getLogger(__name__)


def _is_google_sheets_url(source: str) -> bool:
    return source.strip().lower().startswith("https://docs.google.com/spreadsheets/")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def main() -> None:
    """
    Форсирует следующий пост: игнорирует таймер, просто шлёт "следующий по очереди"
    и обновляет Excel State + state.json так, будто время подошло сейчас.
    """
    load_dotenv()
    setup_logging()
    logger.info("force_next: принудительная отправка следующего поста.")

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token:
        raise SystemExit("Не задан TELEGRAM_BOT_TOKEN (создайте .env по примеру .env.example).")

    posts_source_raw = os.getenv("POSTS_XLSX_PATH", "posts.xlsx").strip()
    posts_source = posts_source_raw
    if not _is_google_sheets_url(posts_source):
        posts_source = str(Path(posts_source).resolve())
    sheet_name = os.getenv("POSTS_SHEET_NAME", "posts").strip()
    state_path = Path(os.getenv("STATE_PATH", "state.json")).resolve()

    posts = load_posts(source=posts_source, sheet_name=sheet_name)
    posts_by_id = index_posts_by_id(posts)
    tg = TelegramClient(token=token)

    use_excel_meta = has_meta_sheets(posts_source)
    if _is_google_sheets_url(posts_source_raw) and not use_excel_meta:
        logger.warning(
            "POSTS_XLSX_PATH указывает на Google Sheets, но листы State / Queue / Settings не доступны через API. "
            "Обычно: нет GOOGLE_SERVICE_ACCOUNT_JSON(_INLINE) в .env на сервере или таблица не расшарена на email "
            "сервис-аккаунта. Лист Posts может читаться через публичный CSV, а очередь и счётчик — из state.json, "
            "не из таблицы."
        )
    if not chat_id and use_excel_meta:
        chat_id = read_settings_chat_id(posts_source)
    if not chat_id:
        raise SystemExit("Не задан TELEGRAM_CHAT_ID и не найден Settings/chat_id в Excel.")

    time_state = BotState.load(state_path)

    step: int | None = None
    q_meta: list[str] = []
    state_for_send = None
    if use_excel_meta:
        q_meta = read_queue_post_ids(posts_source)
        state_for_send = read_state(posts_source)
        step = ((state_for_send.post_index - 1) % len(q_meta)) + 1
        post_id = q_meta[step - 1]
        post = posts_by_id.get(str(post_id))
        if post is None:
            if str(post_id).strip().casefold() == "recycle":
                logger.info("force_next: recycle — сброс Postindex на 1.")
                write_state(posts_source, post_index=1, last_posted_at=_utc_now())
                q_meta = read_queue_post_ids(posts_source)
                first_id = q_meta[0]
                post = posts_by_id.get(str(first_id))
                step = 1
            if post is None:
                raise SystemExit(f"Queue ссылается на PostID={post_id}, но такого ID нет в листе Posts.")
    else:
        idx = time_state.index % len(posts)
        post = posts[idx]

    # Отправляем прямо сейчас, игнорируя интервалы.
    if use_excel_meta and step is not None and state_for_send is not None:
        _send_post(
            tg,
            chat_id,
            post,
            queue_step=step,
            queue_len=len(q_meta),
            excel_post_index=state_for_send.post_index,
        )
    else:
        _send_post(tg, chat_id, post)

    now = _utc_now()

    # Обновляем Excel State (если есть)
    if use_excel_meta:
        q = read_queue_post_ids(posts_source)
        s = read_state(posts_source)
        next_step = (s.post_index % len(q)) + 1
        write_state(posts_source, post_index=next_step, last_posted_at=now)
        logger.info("force_next: State обновлён — Postindex=%s, LastPostedAt записан.", next_step)
    else:
        new_state = BotState(index=(time_state.index + 1) % len(posts), last_posted_at=now)
        new_state.save(state_path)
        logger.info("force_next: state.json — следующий индекс=%s.", new_state.index)

    # Таймер следующего окна по-прежнему в state.json (last_posted_at), даже при Google Sheets.
    forced_state = BotState(index=time_state.index, last_posted_at=now)
    forced_state.save(state_path)
    logger.info("force_next: state.json обновлён (last_posted_at для интервала). Готово.")


if __name__ == "__main__":
    main()


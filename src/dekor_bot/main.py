from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Tuple

from dotenv import load_dotenv

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

from .excel_meta import (
    has_meta_sheets,
    read_frequency_days,
    read_queue_post_ids,
    read_settings_chat_id,
    read_state,
    write_state,
)
from .excel_posts import Post, index_posts_by_id, load_posts
from .state import BotState
from .telegram_api import TelegramClient

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Вызвать один раз при старте CLI (main / force_next). Уровень: LOG_LEVEL или INFO."""
    root = logging.getLogger()
    if root.handlers:
        return
    level_name = (os.getenv("LOG_LEVEL") or "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _is_google_sheets_url(source: str) -> bool:
    return source.strip().lower().startswith("https://docs.google.com/spreadsheets/")


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


try:
    _MSK_TZ = ZoneInfo("Europe/Moscow") if ZoneInfo is not None else timezone(timedelta(hours=3))
except Exception:  # pragma: no cover
    _MSK_TZ = timezone(timedelta(hours=3))


def _parse_post_time_msk(v: str) -> Tuple[int, int]:
    """
    Разбирает время вида "10:00" (или "10") в час/минуты.
    """
    s = (v or "").strip()
    if not s:
        return 10, 0
    if ":" not in s:
        return int(s), 0
    hh_s, mm_s = s.split(":", 1)
    return int(hh_s), int(mm_s)


def _next_post_at_utc_from_last(last_posted_at_utc: datetime, interval_days: int, post_hour: int, post_minute: int) -> datetime:
    """
    Следующий пост всегда в заданное локальное время МСК.
    Интервал применяем к ДАТЕ (в МСК), а не к моменту отправки.
    """
    last_msk = last_posted_at_utc.astimezone(_MSK_TZ)
    next_date_msk = last_msk.date() + timedelta(days=interval_days)
    next_msk = datetime(
        next_date_msk.year,
        next_date_msk.month,
        next_date_msk.day,
        post_hour,
        post_minute,
        tzinfo=_MSK_TZ,
    )
    return next_msk.astimezone(timezone.utc)


def _sleep_seconds_until(next_at: datetime) -> int:
    now = _utc_now()
    delta = next_at - now
    return max(0, int(delta.total_seconds()))


def _all_urls(items: list[str]) -> bool:
    for s in items:
        t = s.strip().lower()
        if not (t.startswith("http://") or t.startswith("https://")):
            return False
    return True


def _preview_text(text: str, max_len: int = 120) -> str:
    one = " ".join(text.split())
    if len(one) <= max_len:
        return one
    return one[: max_len - 1] + "…"


def _send_post(
    tg: TelegramClient,
    chat_id: str,
    post: Post,
    *,
    queue_step: int | None = None,
    queue_len: int | None = None,
    excel_post_index: int | None = None,
) -> None:
    text = (post.text or "").strip()
    photos = post.photos
    videos = post.videos

    extra = ""
    if queue_step is not None and queue_len is not None:
        extra += f", шаг очереди {queue_step}/{queue_len}"
    if excel_post_index is not None:
        extra += f", State.post_index={excel_post_index}"

    if not photos and not videos:
        if not text:
            logger.warning("Пост id=%s пропущен: нет текста и медиа%s", post.post_id, extra)
            return
        logger.info("Отправка: id=%s, chat=%s, только текст (%s симв.)%s", post.post_id, chat_id, len(text), extra)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Текст: %s", _preview_text(text))
        tg.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        logger.info("Отправлено в Telegram (сообщение).")
        return

    media_items = [{"type": "photo", "media": p} for p in photos] + [{"type": "video", "media": v} for v in videos]
    logger.info(
        "Отправка: id=%s, chat=%s, фото=%s видео=%s, альбом=%s%s",
        post.post_id,
        chat_id,
        len(photos),
        len(videos),
        len(media_items) <= 10 and _all_urls([m["media"] for m in media_items]),
        extra,
    )
    if text and logger.isEnabledFor(logging.DEBUG):
        logger.debug("Подпись: %s", _preview_text(text))

    # Если все медиа — URL и <=10 штук, то отправляем альбомом (caption только у первого).
    if len(media_items) <= 10 and _all_urls([m["media"] for m in media_items]):
        if text:
            media_items[0]["caption"] = text
            media_items[0]["parse_mode"] = "HTML"
        tg.send_media_group(chat_id=chat_id, media=media_items)
        logger.info("Отправлено в Telegram (медиагруппа).")
        return

    # Иначе отправляем по одному (чтобы поддержать локальные файлы тоже).
    first_caption = text if text else None
    caption_used = False
    for p in photos:
        tg.send_photo(chat_id=chat_id, photo=p, caption=(first_caption if not caption_used else None), parse_mode="HTML")
        caption_used = caption_used or bool(first_caption)
    for v in videos:
        tg.send_video(chat_id=chat_id, video=v, caption=(first_caption if not caption_used else None), parse_mode="HTML")
        caption_used = caption_used or bool(first_caption)

    # Если текста много и не удалось прикрепить (например, медиа есть, но caption нельзя/не прошло),
    # в конце продублируем текст отдельным сообщением.
    if text and not caption_used:
        tg.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
    logger.info("Отправлено в Telegram (медиа по одному%s).", ", дубль текста" if text and not caption_used else "")


def main() -> None:
    load_dotenv()
    setup_logging()

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token:
        raise SystemExit("Не задан TELEGRAM_BOT_TOKEN (создайте .env по примеру .env.example).")

    posts_source_raw = os.getenv("POSTS_XLSX_PATH", "posts.xlsx").strip()
    posts_source = posts_source_raw
    if not _is_google_sheets_url(posts_source):
        posts_source = str(Path(posts_source).resolve())
    sheet_name = os.getenv("POSTS_SHEET_NAME", "posts").strip()
    interval_days = int(os.getenv("INTERVAL_DAYS", "2"))
    post_time_msk = os.getenv("POST_TIME_MSK", "10:00").strip()
    post_hour, post_minute = _parse_post_time_msk(post_time_msk)
    start_immediately = _env_bool("START_IMMEDIATELY", True)
    run_once = _env_bool("RUN_ONCE", False)
    state_path = Path(os.getenv("STATE_PATH", "state.json")).resolve()

    posts = load_posts(source=posts_source, sheet_name=sheet_name)
    posts_by_id = index_posts_by_id(posts)
    tg = TelegramClient(token=token)

    use_excel_meta = has_meta_sheets(posts_source)
    if _is_google_sheets_url(posts_source_raw) and not use_excel_meta:
        logger.warning(
            "POSTS_XLSX_PATH — Google Sheets, но State/Queue/Settings не открываются через API "
            "(проверьте GOOGLE_SERVICE_ACCOUNT_JSON или GOOGLE_SERVICE_ACCOUNT_JSON_INLINE в .env и доступ к таблице). "
            "Посты могут грузиться с публичного CSV; счётчик очереди — из state.json."
        )
    if use_excel_meta:
        freq = read_frequency_days(posts_source)
        if freq is not None:
            interval_days = int(freq)
    if not chat_id and use_excel_meta:
        chat_id = read_settings_chat_id(posts_source)
    if not chat_id:
        raise SystemExit("Не задан TELEGRAM_CHAT_ID и не найден Settings/chat_id в Excel.")

    logger.info(
        "Старт: источник=%s, meta_sheets=%s, интервал=%s дн., время МСК=%02d:%02d, RUN_ONCE=%s, START_IMMEDIATELY=%s",
        posts_source if len(str(posts_source)) < 80 else str(posts_source)[:77] + "…",
        use_excel_meta,
        interval_days,
        post_hour,
        post_minute,
        run_once,
        start_immediately,
    )

    while True:
        # Время/интервал держим в state.json (чтобы не ломать Excel),
        # а очередность берём из Excel (State/Queue), если они есть.
        time_state = BotState.load(state_path)
        excel_last_posted_at: datetime | None = None

        if use_excel_meta:
            q = read_queue_post_ids(posts_source)
            s = read_state(posts_source)
            excel_last_posted_at = s.last_posted_at
            step = ((s.post_index - 1) % len(q)) + 1  # 1..len(q)
            post_id = q[step - 1]
            post = posts_by_id.get(str(post_id))
            if post is None:
                # В вашем файле Queue заканчивается маркером "recycle" — это означает "вернуться к началу".
                if str(post_id).strip().casefold() == "recycle":
                    logger.info("Queue: recycle — сброс Postindex на 1.")
                    write_state(posts_source, post_index=1, last_posted_at=_utc_now())
                    continue
                raise SystemExit(f"Queue ссылается на PostID={post_id}, но такого ID нет в листе Posts.")
        else:
            idx = time_state.index % len(posts)
            post = posts[idx]

        # Если в Excel хранится время последней публикации, используем его как источник истины.
        effective_last_posted_at = excel_last_posted_at if excel_last_posted_at is not None else time_state.last_posted_at
        if effective_last_posted_at is None:
            if start_immediately:
                if use_excel_meta:
                    _send_post(
                        tg,
                        chat_id,
                        post,
                        queue_step=step,
                        queue_len=len(q),
                        excel_post_index=s.post_index,
                    )
                    q = read_queue_post_ids(posts_source)
                    s = read_state(posts_source)
                    next_step = (s.post_index % len(q)) + 1
                    write_state(posts_source, post_index=next_step, last_posted_at=_utc_now())
                    logger.info("State обновлён: Postindex=%s, LastPostedAt=сейчас.", next_step)
                else:
                    _send_post(tg, chat_id, post)
                    # индекс в json используем только если нет Excel очереди
                    new_state = BotState(index=(time_state.index + 1) % len(posts), last_posted_at=_utc_now())
                    new_state.save(state_path)
                    logger.info("state.json: следующий индекс поста=%s.", new_state.index)

                new_time_state = BotState(index=time_state.index, last_posted_at=_utc_now())
                new_time_state.save(state_path)
                if run_once:
                    logger.info("RUN_ONCE: старт с немедленной отправкой — выход.")
                    return
                continue
            # Первый запуск: если не стартуем сразу — ждём ближайшие постовые часы в МСК.
            now_msk = _utc_now().astimezone(_MSK_TZ)
            today_target = datetime(
                now_msk.year,
                now_msk.month,
                now_msk.day,
                post_hour,
                post_minute,
                tzinfo=_MSK_TZ,
            )
            if now_msk < today_target:
                next_at = today_target.astimezone(timezone.utc)
            else:
                next_at = (today_target + timedelta(days=1)).astimezone(timezone.utc)
        else:
            next_at = _next_post_at_utc_from_last(
                last_posted_at_utc=effective_last_posted_at,
                interval_days=interval_days,
                post_hour=post_hour,
                post_minute=post_minute,
            )

        sleep_s = _sleep_seconds_until(next_at)
        if sleep_s > 0:
            if run_once:
                logger.info(
                    "Пока рано: следующий слот %s UTC (%s МСК), осталось ~%s мин. RUN_ONCE — выход.",
                    next_at.strftime("%Y-%m-%d %H:%M:%S %z"),
                    next_at.astimezone(_MSK_TZ).strftime("%Y-%m-%d %H:%M"),
                    max(1, sleep_s // 60),
                )
                return
            logger.debug(
                "Ожидание: ~%s с до %s UTC",
                min(sleep_s, 60),
                next_at.strftime("%H:%M:%S"),
            )
            time.sleep(min(sleep_s, 60))
            continue

        if use_excel_meta:
            _send_post(
                tg,
                chat_id,
                post,
                queue_step=step,
                queue_len=len(q),
                excel_post_index=s.post_index,
            )
        else:
            _send_post(tg, chat_id, post)

        if use_excel_meta:
            q = read_queue_post_ids(posts_source)
            s = read_state(posts_source)
            next_step = (s.post_index % len(q)) + 1
            write_state(posts_source, post_index=next_step, last_posted_at=_utc_now())
            logger.info("State обновлён: Postindex=%s.", next_step)
        else:
            new_state = BotState(index=(time_state.index + 1) % len(posts), last_posted_at=_utc_now())
            new_state.save(state_path)
            logger.info("state.json: следующий индекс поста=%s.", new_state.index)

        new_time_state = BotState(index=time_state.index, last_posted_at=_utc_now())
        new_time_state.save(state_path)
        if run_once:
            logger.info("RUN_ONCE: цикл завершён после публикации.")
            return


if __name__ == "__main__":
    main()


from __future__ import annotations

import logging
import os
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Tuple

from dotenv import load_dotenv

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

from .excel_meta import (
    has_meta_sheets,
    highlight_post_row_pastel_red,
    read_frequency_days,
    read_queue_post_ids,
    read_settings_chat_id,
    read_state,
    record_failed_post_stat,
    write_state,
)
from .excel_posts import Post, index_posts_by_id, load_posts
from .post_failures import (
    explain_post_failure,
    notify_admin_about_failed_post,
    post_max_attempts,
    post_retry_delay_sec,
)
from .run_log import append_run_log
from .media_sync import sync_media_if_enabled
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


def _default_lock_path() -> str:
    data_dir = Path((os.getenv("DATA_DIR") or "data").strip())
    data_dir.mkdir(parents=True, exist_ok=True)
    return str(data_dir / "dekor_autoposting.lock")


def _acquire_single_instance_lock():
    """
    Гарантирует единственный активный процесс бота на хосте.
    Если lock уже занят, завершаемся без отправки (защита от дублей).
    """
    lock_path = (os.getenv("BOT_LOCK_FILE") or _default_lock_path()).strip()
    Path(lock_path).parent.mkdir(parents=True, exist_ok=True)
    lock_file = open(lock_path, "a+", encoding="utf-8")
    try:
        if os.name == "nt":
            import msvcrt  # pragma: no cover

            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception:
        lock_file.close()
        raise SystemExit(f"Уже запущен другой экземпляр бота (lock: {lock_path}).")

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    return lock_file


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
    Интервал применяем к дате (в МСК), а не к моменту отправки.
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


def _require_meta_sheets(posts_source: str, posts_source_raw: str) -> None:
    if has_meta_sheets(posts_source):
        return
    hint_gs = ""
    if _is_google_sheets_url(posts_source_raw):
        hint_gs = (
            " Для Google Sheets: задайте GOOGLE_SERVICE_ACCOUNT_JSON или GOOGLE_SERVICE_ACCOUNT_JSON_INLINE, "
            "расшарьте таблицу на email сервис-аккаунта (редактор)."
        )
    raise SystemExit(
        "В книге (POSTS_XLSX_PATH) обязательны листы State, Queue и Settings. "
        "Счётчик и время последней публикации хранятся только в листе State (Postindex, LastPostedAt)."
        + hint_gs
    )


def _fmt_dt_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %z")


def _fmt_dt_msk(dt: datetime) -> str:
    return dt.astimezone(_MSK_TZ).strftime("%Y-%m-%d %H:%M")


def _base_run_log(
    *,
    started_at: datetime,
    interval_days: int,
    post_hour: int,
    post_minute: int,
    run_once: bool,
    start_immediately: bool,
    posts_source: str,
) -> dict[str, Any]:
    return {
        "started_at": started_at.isoformat(),
        "interval_days": interval_days,
        "post_time_msk": f"{post_hour:02d}:{post_minute:02d}",
        "run_once": run_once,
        "start_immediately": start_immediately,
        "posts_source": posts_source if len(posts_source) < 120 else posts_source[:117] + "…",
    }


def _log_run(entry: dict[str, Any]) -> None:
    try:
        append_run_log(entry)
    except Exception:
        logger.exception("Не удалось записать run_log.json")


def _advance_queue_on_success(posts_source: str, queue_len: int) -> int:
    s = read_state(posts_source)
    next_step = (s.post_index % queue_len) + 1
    write_state(posts_source, post_index=next_step, last_posted_at=_utc_now())
    return next_step


def _advance_queue_on_skip(posts_source: str, queue_len: int) -> int:
    """Пропуск битого поста: сдвигаем очередь, LastPostedAt не трогаем — слот ещё открыт."""
    s = read_state(posts_source)
    next_step = (s.post_index % queue_len) + 1
    write_state(posts_source, post_index=next_step, last_posted_at=s.last_posted_at)
    return next_step


def _attempt_send_post(
    tg: TelegramClient,
    chat_id: str,
    post: Post,
    *,
    queue_step: int,
    queue_len: int,
    excel_post_index: int,
) -> tuple[bool, Exception | None]:
    attempts = post_max_attempts()
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            _send_post(
                tg,
                chat_id,
                post,
                queue_step=queue_step,
                queue_len=queue_len,
                excel_post_index=excel_post_index,
            )
            return True, None
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Пост id=%s: попытка %s/%s не удалась: %s",
                post.post_id,
                attempt,
                attempts,
                exc,
            )
            if attempt < attempts:
                time.sleep(post_retry_delay_sec())
    return False, last_exc


def _handle_failed_post(
    tg: TelegramClient,
    posts_source: str,
    posts_sheet_name: str,
    post: Post,
    *,
    queue_step: int,
    queue_len: int,
    exc: Exception,
    cycle_log: dict[str, Any],
    action: str,
    slot_msk: str | None = None,
) -> int:
    short_reason, advice = explain_post_failure(exc, post)
    attempts = post_max_attempts()

    try:
        record_failed_post_stat(posts_source, post.post_id, short_reason)
        highlight_post_row_pastel_red(posts_source, posts_sheet_name, post.post_id)
    except Exception:
        logger.exception("Не удалось записать статистику/подсветку для поста %s", post.post_id)

    try:
        notify_admin_about_failed_post(
            tg,
            post,
            queue_step=queue_step,
            queue_len=queue_len,
            short_reason=short_reason,
            advice=advice,
            attempts=attempts,
        )
    except Exception:
        logger.exception("Не удалось уведомить админа о падении поста %s", post.post_id)

    next_step = _advance_queue_on_skip(posts_source, queue_len)
    msg = f"Пост #{post.post_id} пропущен после {attempts} попыток: {short_reason}"
    if slot_msk:
        msg = f"{msg} (слот {slot_msk} МСК)."
    logger.error(msg)

    _log_run(
        {
            **cycle_log,
            "status": "skipped",
            "action": action,
            "message": msg,
            "error": str(exc),
            "failure_reason": short_reason,
            "attempts": attempts,
            "finished_at": _utc_now().isoformat(),
        }
    )
    return next_step


def _cycle_error_backoff_sec() -> int:
    try:
        n = int(os.getenv("CYCLE_ERROR_BACKOFF_SEC", "60"))
    except ValueError:
        n = 60
    return max(10, n)


def _reload_posts(posts_source: str, sheet_name: str) -> dict[str, Post]:
    posts = load_posts(source=posts_source, sheet_name=sheet_name)
    return index_posts_by_id(posts)


def main() -> None:
    load_dotenv()
    setup_logging()
    started_at = _utc_now()
    run_log: dict[str, Any] = {"status": "startup_error"}
    cycle_logged = False

    try:
        _instance_lock = _acquire_single_instance_lock()

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

        posts_by_id = _reload_posts(posts_source, sheet_name)
        tg = TelegramClient(token=token)

        _require_meta_sheets(posts_source, posts_source_raw)

        if sync_media_if_enabled(posts_source, sheet_name):
            logger.info("Media sync: таблица обновлена — перечитываем посты.")
            posts_by_id = _reload_posts(posts_source, sheet_name)

        freq = read_frequency_days(posts_source)
        if freq is not None:
            interval_days = int(freq)
        if not chat_id:
            chat_id = read_settings_chat_id(posts_source)
        if not chat_id:
            raise SystemExit("Не задан TELEGRAM_CHAT_ID и не найден chat_id в листе Settings.")

        run_log = _base_run_log(
            started_at=started_at,
            interval_days=interval_days,
            post_hour=post_hour,
            post_minute=post_minute,
            run_once=run_once,
            start_immediately=start_immediately,
            posts_source=str(posts_source),
        )

        logger.info(
            "Старт: источник=%s, прогресс в листе State, интервал=%s дн., время МСК=%02d:%02d, RUN_ONCE=%s, START_IMMEDIATELY=%s",
            posts_source if len(str(posts_source)) < 80 else str(posts_source)[:77] + "…",
            interval_days,
            post_hour,
            post_minute,
            run_once,
            start_immediately,
        )

        consecutive_skips = 0

        while True:
            try:
                if sync_media_if_enabled(posts_source, sheet_name):
                    logger.info("Media sync: таблица обновлена — перечитываем посты.")
                posts_by_id = _reload_posts(posts_source, sheet_name)
                q = read_queue_post_ids(posts_source)
                s = read_state(posts_source)
                excel_last_posted_at = s.last_posted_at
                step = ((s.post_index - 1) % len(q)) + 1  # 1..len(q)
                post_id = q[step - 1]
                post = posts_by_id.get(str(post_id))
                if post is None:
                    if str(post_id).strip().casefold() == "recycle":
                        logger.info("Queue: recycle — сброс Postindex на 1.")
                        write_state(posts_source, post_index=1, last_posted_at=_utc_now())
                        continue
                    msg = f"Queue ссылается на PostID={post_id}, но такого ID нет в листе Posts."
                    logger.error(msg)
                    if run_once:
                        raise SystemExit(msg)
                    _advance_queue_on_skip(posts_source, len(q))
                    time.sleep(_cycle_error_backoff_sec())
                    continue

                cycle_log: dict[str, Any] = {
                    **run_log,
                    "post_id": str(post_id),
                    "queue_step": step,
                    "queue_len": len(q),
                    "post_index": s.post_index,
                    "last_posted_at": excel_last_posted_at.isoformat() if excel_last_posted_at else None,
                }

                effective_last_posted_at = excel_last_posted_at
                if effective_last_posted_at is None:
                    if start_immediately:
                        ok, exc = _attempt_send_post(
                            tg,
                            chat_id,
                            post,
                            queue_step=step,
                            queue_len=len(q),
                            excel_post_index=s.post_index,
                        )
                        if ok:
                            consecutive_skips = 0
                            next_step = _advance_queue_on_success(posts_source, len(q))
                            logger.info("State обновлён: Postindex=%s, LastPostedAt=сейчас.", next_step)
                            _log_run(
                                {
                                    **cycle_log,
                                    "status": "success",
                                    "action": "immediate_first_post",
                                    "message": "Первый пост отправлен сразу (START_IMMEDIATELY=true).",
                                    "finished_at": _utc_now().isoformat(),
                                }
                            )
                            cycle_logged = True
                            if run_once:
                                logger.info("RUN_ONCE: старт с немедленной отправкой — выход.")
                                return
                            continue
                        assert exc is not None
                        _handle_failed_post(
                            tg,
                            posts_source,
                            sheet_name,
                            post,
                            queue_step=step,
                            queue_len=len(q),
                            exc=exc,
                            cycle_log=cycle_log,
                            action="immediate_first_post_failed",
                        )
                        cycle_logged = True
                        consecutive_skips += 1
                        if consecutive_skips >= len(q):
                            logger.error("Все посты в очереди упали подряд — останавливаем попытки.")
                            s = read_state(posts_source)
                            write_state(posts_source, post_index=s.post_index, last_posted_at=_utc_now())
                            if run_once:
                                return
                            continue
                        logger.info("Упавший пост пропущен — сразу пробуем следующий.")
                        continue
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
                cycle_log.update(
                    {
                        "scheduled_slot_utc": _fmt_dt_utc(next_at),
                        "scheduled_slot_msk": _fmt_dt_msk(next_at),
                        "seconds_until_slot": sleep_s,
                        "is_due": sleep_s == 0,
                    }
                )

                if sleep_s > 0:
                    if run_once:
                        msg = (
                            f"Запланировано на {_fmt_dt_msk(next_at)} МСК — пока рано, "
                            f"осталось ~{max(1, sleep_s // 60)} мин."
                        )
                        logger.info(
                            "Пока рано: следующий слот %s UTC (%s МСК), осталось ~%s мин. RUN_ONCE — выход.",
                            next_at.strftime("%Y-%m-%d %H:%M:%S %z"),
                            next_at.astimezone(_MSK_TZ).strftime("%Y-%m-%d %H:%M"),
                            max(1, sleep_s // 60),
                        )
                        _log_run(
                            {
                                **cycle_log,
                                "status": "scheduled_skip",
                                "action": "wait_for_slot",
                                "message": msg,
                                "finished_at": _utc_now().isoformat(),
                            }
                        )
                        cycle_logged = True
                        return
                    logger.debug(
                        "Ожидание: ~%s с до %s UTC",
                        min(sleep_s, 60),
                        next_at.strftime("%H:%M:%S"),
                    )
                    time.sleep(min(sleep_s, 60))
                    continue

                ok, exc = _attempt_send_post(
                    tg,
                    chat_id,
                    post,
                    queue_step=step,
                    queue_len=len(q),
                    excel_post_index=s.post_index,
                )
                if ok:
                    consecutive_skips = 0
                    next_step = _advance_queue_on_success(posts_source, len(q))
                    logger.info("State обновлён: Postindex=%s.", next_step)
                    _log_run(
                        {
                            **cycle_log,
                            "status": "success",
                            "action": "scheduled_post",
                            "message": f"Отчёт успешно отправлен в запланированный слот {_fmt_dt_msk(next_at)} МСК.",
                            "finished_at": _utc_now().isoformat(),
                        }
                    )
                    cycle_logged = True
                    if run_once:
                        logger.info("RUN_ONCE: цикл завершён после публикации.")
                        return
                    continue

                assert exc is not None
                _handle_failed_post(
                    tg,
                    posts_source,
                    sheet_name,
                    post,
                    queue_step=step,
                    queue_len=len(q),
                    exc=exc,
                    cycle_log=cycle_log,
                    action="scheduled_post_failed",
                    slot_msk=_fmt_dt_msk(next_at),
                )
                cycle_logged = True
                consecutive_skips += 1
                if consecutive_skips >= len(q):
                    logger.error("Все посты в очереди упали подряд в этом слоте — ждём следующий интервал.")
                    s = read_state(posts_source)
                    write_state(posts_source, post_index=s.post_index, last_posted_at=_utc_now())
                    if run_once:
                        return
                    continue
                logger.info("Упавший пост пропущен — в этот же слот пробуем следующий.")
                continue
            except SystemExit:
                raise
            except Exception as exc:
                logger.exception(
                    "Ошибка в цикле бота — ждём %s с и пробуем снова (процесс не завершаем): %s",
                    _cycle_error_backoff_sec(),
                    exc,
                )
                _log_run(
                    {
                        **run_log,
                        "status": "error",
                        "action": "cycle_recover",
                        "message": "Временная ошибка цикла, бот продолжит работу.",
                        "error": str(exc),
                        "finished_at": _utc_now().isoformat(),
                    }
                )
                if run_once:
                    raise
                time.sleep(_cycle_error_backoff_sec())
                continue
    except SystemExit as exc:
        run_log.update(
            {
                "status": "error",
                "message": str(exc) or "Завершение с ошибкой.",
                "error": str(exc),
                "finished_at": _utc_now().isoformat(),
            }
        )
        _log_run(run_log)
        raise
    except Exception as exc:
        if not cycle_logged:
            run_log.update(
                {
                    "status": "error",
                    "message": "Непредвиденная ошибка при запуске.",
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                    "finished_at": _utc_now().isoformat(),
                }
            )
            _log_run(run_log)
        raise


if __name__ == "__main__":
    main()

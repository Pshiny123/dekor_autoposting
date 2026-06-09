from __future__ import annotations

import logging
import os
import re

from .excel_posts import Post
from .telegram_api import TelegramClient

logger = logging.getLogger(__name__)

def admin_chat_id() -> str:
    return (os.getenv("TELEGRAM_ADMIN_CHAT_ID") or "253593784").strip()


def post_max_attempts() -> int:
    try:
        n = int(os.getenv("POST_MAX_ATTEMPTS", "3"))
    except ValueError:
        n = 3
    return max(1, n)


def post_retry_delay_sec() -> int:
    try:
        n = int(os.getenv("POST_RETRY_DELAY_SEC", "3"))
    except ValueError:
        n = 3
    return max(1, n)


def _extract_telegram_description(exc: Exception) -> str:
    text = str(exc)
    m = re.search(r"'description':\s*'([^']+)'", text)
    if m:
        return m.group(1)
    m = re.search(r'"description":\s*"([^"]+)"', text)
    if m:
        return m.group(1)
    return text


def _media_summary(post: Post) -> str:
    parts: list[str] = []
    for i, url in enumerate(post.photos[:3], start=1):
        parts.append(f"Photo{i}: {url[:120]}{'…' if len(url) > 120 else ''}")
    for i, url in enumerate(post.videos[:3], start=1):
        parts.append(f"Video{i}: {url[:120]}{'…' if len(url) > 120 else ''}")
    if len(post.photos) > 3:
        parts.append(f"…ещё фото: {len(post.photos) - 3}")
    if len(post.videos) > 3:
        parts.append(f"…ещё видео: {len(post.videos) - 3}")
    return "\n".join(parts) if parts else "медиа нет"


def explain_post_failure(exc: Exception, post: Post) -> tuple[str, str]:
    """
    Возвращает (краткая причина для таблицы, развёрнутые рекомендации для админа).
    """
    desc = _extract_telegram_description(exc)
    upper = desc.upper()
    media = _media_summary(post)

    if "WEBPAGE_MEDIA_EMPTY" in upper:
        short = "Битая ссылка на медиа (WEBPAGE_MEDIA_EMPTY)"
        advice = (
            "Telegram не смог скачать файл по URL.\n"
            "Что проверить:\n"
            "• ссылка открывается в браузере без авторизации;\n"
            "• для Google Drive / Dropbox — прямая ссылка на файл, не страница просмотра;\n"
            "• видео не удалено и не приватное;\n"
            "• в ячейках Photo/Video нет лишних пробелов.\n"
            f"Медиа поста:\n{media}"
        )
        return short, advice

    if "WRONG FILE IDENTIFIER" in upper or "FILE_ID" in upper:
        short = "Некорректный идентификатор/URL медиа"
        advice = (
            "Ссылка на фото/видео невалидна для Telegram.\n"
            "Замените URL на рабочую прямую ссылку или загрузите файл на хостинг с прямым доступом.\n"
            f"Медиа поста:\n{media}"
        )
        return short, advice

    if "CAN'T PARSE ENTITIES" in upper or "CANT PARSE ENTITIES" in upper:
        short = "Ошибка HTML-разметки в тексте"
        advice = (
            "В колонке text некорректный HTML для Telegram.\n"
            "Проверьте незакрытые теги <b>, <i>, <a> и спецсимволы < > &.\n"
            "Исправьте подпись поста или упростите форматирование."
        )
        return short, advice

    if "MESSAGE IS TOO LONG" in upper:
        short = "Текст поста слишком длинный"
        advice = "Сократите текст в колонке text (лимит Telegram для подписи/сообщения)."
        return short, advice

    if "CHAT NOT FOUND" in upper:
        short = "Чат/канал не найден"
        advice = "Проверьте TELEGRAM_CHAT_ID в Settings/.env и что бот добавлен в канал как админ."
        return short, advice

    if "BOT WAS BLOCKED" in upper:
        short = "Бот заблокирован получателем"
        advice = "Получатель заблокировал бота — для канала проверьте права бота."
        return short, advice

    if "TIMED OUT" in upper or "TIMEOUT" in upper:
        short = "Таймаут при отправке"
        advice = "Сеть или Telegram API не ответили вовремя. Можно повторить позже или проверить доступ к api.telegram.org."
        return short, advice

    if not post.text.strip() and not post.photos and not post.videos:
        short = "Пустой пост: нет текста и медиа"
        advice = "Заполните text или добавьте Photo/Video в лист Posts."
        return short, advice

    short = desc[:200] if desc else type(exc).__name__
    advice = (
        f"Техническая ошибка: {desc}\n"
        f"Пост ID={post.post_id}. Проверьте текст и ссылки на медиа.\n"
        f"Медиа:\n{media}"
    )
    return short, advice


def build_admin_alert(
    post: Post,
    *,
    queue_step: int,
    queue_len: int,
    short_reason: str,
    advice: str,
    attempts: int,
) -> str:
    return (
        "💩💩💩 ‼️ УПАЛ ПОСТ ‼️ 💩💩💩\n"
        f"‼️ Пост <b>#{post.post_id}</b> не отправился после <b>{attempts}</b> попыток ‼️\n\n"
        f"Очередь: шаг {queue_step}/{queue_len}\n"
        f"<b>Причина:</b> {short_reason}\n\n"
        f"<b>Что сделать:</b>\n{advice}\n\n"
        "Бот сразу пробует отправить следующий пост в этот же слот."
    )


def notify_admin_about_failed_post(
    tg: TelegramClient,
    post: Post,
    *,
    queue_step: int,
    queue_len: int,
    short_reason: str,
    advice: str,
    attempts: int,
) -> None:
    admin_id = admin_chat_id()
    if not admin_id:
        logger.warning("TELEGRAM_ADMIN_CHAT_ID не задан — уведомление админу пропущено.")
        return
    text = build_admin_alert(
        post,
        queue_step=queue_step,
        queue_len=queue_len,
        short_reason=short_reason,
        advice=advice,
        attempts=attempts,
    )
    tg.send_message(chat_id=admin_id, text=text, parse_mode="HTML")
    logger.info("Админу %s отправлено уведомление об упавшем посте #%s.", admin_id, post.post_id)

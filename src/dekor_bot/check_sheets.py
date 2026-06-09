"""
Проверка доступа сервис-аккаунта к Google Таблице (те же .env, что у бота).

Телеграм-бота в таблицу добавлять не нужно — доступ даётся email из ключа
client_email в JSON сервис-аккаунта (Google Sheets → Поделиться → редактор).

Запуск из каталога проекта:
  python -m src.dekor_bot.check_sheets
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from dotenv import load_dotenv


def _service_account_email() -> str | None:
    import os

    from .excel_meta import normalize_google_service_account_json_inline

    path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if path:
        p = Path(path).expanduser()
        if p.is_file():
            try:
                return str(json.loads(p.read_text(encoding="utf-8")).get("client_email") or "").strip() or None
            except (OSError, json.JSONDecodeError):
                pass
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_INLINE", "")
    inline = normalize_google_service_account_json_inline(raw)
    if inline:
        try:
            return str(json.loads(inline).get("client_email") or "").strip() or None
        except json.JSONDecodeError:
            return None
    return None


def main() -> int:
    import os

    load_dotenv()

    try:
        import gspread  # noqa: F401
    except ImportError:
        print(
            "Модуль gspread не установлен. В активированном venv выполните:\n"
            "  pip install -r requirements.txt\n"
            "или:\n"
            "  pip install gspread google-auth",
            file=sys.stderr,
        )
        return 1

    from .excel_meta import (
        _extract_gsheet_id,
        _get_gspread_client,
        _is_google_sheets_url,
        _norm,
        has_meta_sheets,
        normalize_google_service_account_json_inline,
        read_queue_post_ids,
        read_state,
    )

    raw = os.getenv("POSTS_XLSX_PATH", "").strip()
    if not raw:
        print("В .env не задан POSTS_XLSX_PATH.", file=sys.stderr)
        return 1
    if not _is_google_sheets_url(raw):
        print("POSTS_XLSX_PATH не похож на URL Google Таблицы — проверка API Sheets не применима.")
        print(f"Значение: {raw[:100]}{'…' if len(raw) > 100 else ''}")
        return 0

    email = _service_account_email()
    inline_raw = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_INLINE") or "").strip()
    json_path = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    json_file = Path(json_path).expanduser() if json_path else None

    if email:
        print(f"Сервис-аккаунт (добавьте его в «Поделиться» таблицы как редактор): {email}")
    elif json_path and json_file and json_file.is_file():
        print(
            f"Файл {json_path} есть, но это не валидный JSON сервис-аккаунта (или нет client_email). Перезапишите файл из Google.",
            file=sys.stderr,
        )
    elif json_path and json_file and not json_file.is_file():
        print(
            f"В .env задан GOOGLE_SERVICE_ACCOUNT_JSON={json_path}, но файла нет. Создайте его или поправьте путь.",
            file=sys.stderr,
        )
    elif inline_raw:
        print(
            "Битый GOOGLE_SERVICE_ACCOUNT_JSON_INLINE и не задан рабочий GOOGLE_SERVICE_ACCOUNT_JSON (файл).\n"
            "  В .env добавьте строку: GOOGLE_SERVICE_ACCOUNT_JSON=/opt/dekor_autoposting/sa.json",
            file=sys.stderr,
        )
    else:
        print(
            "Не заданы GOOGLE_SERVICE_ACCOUNT_JSON и GOOGLE_SERVICE_ACCOUNT_JSON_INLINE.",
            file=sys.stderr,
        )

    try:
        client = _get_gspread_client()
    except Exception as exc:
        print(f"Ошибка учётных данных Google: {exc}", file=sys.stderr)
        if inline_raw:
            n = normalize_google_service_account_json_inline(inline_raw)
            print(
                f"Подсказка: длина значения INLINE после нормализации — {len(n)} симв.; "
                "если <<300, строка в .env, скорее всего, обрезана (или лишние внешние кавычки).",
                file=sys.stderr,
            )
        if not email and not inline_raw and not json_path:
            print("(Задайте корректный JSON сервис-аккаунта — см. выше.)", file=sys.stderr)
        return 1

    sid = _extract_gsheet_id(raw)
    try:
        sh = client.open_by_key(sid)
    except Exception as exc:
        print(f"Не удалось открыть таблицу по id={sid}: {exc}", file=sys.stderr)
        print("Часто: таблица не расшарена на email сервис-аккаунта или неверный URL.", file=sys.stderr)
        return 1

    titles = [ws.title for ws in sh.worksheets()]
    print(f"Файл: {sh.title}")
    print(f"Листы ({len(titles)}): {', '.join(titles)}")

    need = {"state", "queue", "settings"}
    have = {_norm(t) for t in titles}
    missing = sorted(n.title() for n in need - have)
    if missing:
        print(f"Внимание: по имени (без регистра) не хватает листов, похожих на: {missing}")

    ok = has_meta_sheets(raw)
    print(f"has_meta_sheets (State+Queue+Settings доступны через API): {ok}")

    if ok:
        st = read_state(raw)
        q = read_queue_post_ids(raw)
        print(f"  State: Postindex={st.post_index}, LastPostedAt={st.last_posted_at}")
        print(f"  Queue: длина={len(q)}, первые PostID: {q[:5]}{'…' if len(q) > 5 else ''}")
        print("OK: бот может читать/писать State и очередь через эту таблицу.")
        return 0

    print(
        "Meta-листы недоступны — бот не запустится без листов State, Queue, Settings (см. python -m src.dekor_bot.main).",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

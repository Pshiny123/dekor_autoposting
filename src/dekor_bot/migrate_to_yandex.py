"""
Разовая миграция медиа в Yandex Object Storage (обёртка над media_sync).

  python -m src.dekor_bot.migrate_to_yandex
  python -m src.dekor_bot.migrate_to_yandex --dry-run
"""
from __future__ import annotations

import argparse
import logging
import os

from dotenv import load_dotenv

from .media_sync import load_media_cells, sync_media
from .yandex_storage import bucket_name, is_yandex_url

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    load_dotenv()
    setup_logging()

    parser = argparse.ArgumentParser(description="Миграция медиа в Yandex Object Storage")
    parser.add_argument("--dry-run", action="store_true", help="Только показать, что будет синхронизировано")
    args = parser.parse_args()

    source = os.getenv("POSTS_XLSX_PATH", "").strip()
    sheet = os.getenv("POSTS_SHEET_NAME", "Posts").strip()
    bucket = bucket_name()

    cells = load_media_cells(source, sheet)
    pending = [c for c in cells if c.url and not is_yandex_url(c.url, bucket)]
    logger.info("Ячеек с внешними ссылками к загрузке: %s", len(pending))
    for c in pending:
        logger.info("  #%s %s ← %s", c.post_id, c.col, c.url[:100])

    if args.dry_run:
        logger.info("Dry-run: загрузка не выполнялась.")
        return

    if sync_media(source, sheet):
        logger.info("Миграция завершена, таблица обновлена.")
    else:
        logger.info("Миграция: изменений нет (всё уже в бакете).")


if __name__ == "__main__":
    main()

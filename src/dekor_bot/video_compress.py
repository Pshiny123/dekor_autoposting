"""Сжатие видео до лимита Telegram-бота (~50 МБ) с минимальной потерей качества."""
from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def compress_enabled() -> bool:
    v = os.getenv("VIDEO_COMPRESS_ENABLED")
    if v is None:
        return True
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def max_video_bytes() -> int:
    try:
        mb = float(os.getenv("TELEGRAM_MAX_VIDEO_MB", "49"))
    except ValueError:
        mb = 49.0
    return int(mb * 1024 * 1024)


def ffmpeg_path() -> str:
    custom = (os.getenv("FFMPEG_PATH") or "").strip()
    if custom and Path(custom).is_file():
        return custom
    found = shutil.which("ffmpeg")
    if found:
        return found
    raise RuntimeError(
        "ffmpeg не найден. Установите ffmpeg (apt install ffmpeg / winget install ffmpeg) "
        "или задайте FFMPEG_PATH в .env."
    )


def video_needs_compress(path: Path) -> bool:
    return path.is_file() and path.stat().st_size > max_video_bytes()


def compress_video_for_telegram(src: Path, *, dest: Path | None = None) -> Path:
    """
    Сжимает видео до TELEGRAM_MAX_VIDEO_MB (по умолчанию 49 МБ).
    Подбирает CRF/разрешение — сначала мягче, потом сильнее.
    """
    if not compress_enabled():
        return src

    src = src.resolve()
    limit = max_video_bytes()
    if src.stat().st_size <= limit:
        return src

    out = (dest or src.with_name(f"{src.stem}_tg.mp4")).resolve()
    ffmpeg = ffmpeg_path()
    orig_mb = src.stat().st_size / (1024 * 1024)

    attempts: list[list[str]] = [
        ["-crf", "26", "-preset", "medium", "-vf", "scale='min(1920,iw)':-2"],
        ["-crf", "28", "-preset", "medium", "-vf", "scale='min(1920,iw)':-2"],
        ["-crf", "30", "-preset", "medium", "-vf", "scale='min(1280,iw)':-2"],
        ["-crf", "32", "-preset", "faster", "-vf", "scale='min(960,iw)':-2"],
        ["-crf", "34", "-preset", "faster", "-vf", "scale='min(720,iw)':-2"],
    ]

    last_out: Path | None = None
    for i, extra in enumerate(attempts, start=1):
        try_out = out if i == len(attempts) else out.with_name(f"{out.stem}_try{i}.mp4")
        cmd = [
            ffmpeg,
            "-y",
            "-i",
            str(src),
            "-vcodec",
            "libx264",
            *extra,
            "-acodec",
            "aac",
            "-b:a",
            "128k",
            "-movflags",
            "+faststart",
            str(try_out),
        ]
        logger.info(
            "Сжатие видео для Telegram: %.1f МБ → цель ≤%.0f МБ (попытка %s/%s)…",
            orig_mb,
            limit / (1024 * 1024),
            i,
            len(attempts),
        )
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "")[-500:]
            raise RuntimeError(f"ffmpeg ошибка: {err}")

        size = try_out.stat().st_size
        last_out = try_out
        if size <= limit:
            if try_out != out:
                try_out.replace(out)
            logger.info("Видео сжато: %.1f МБ → %.1f МБ", orig_mb, size / (1024 * 1024))
            return out

    if last_out is None:
        raise RuntimeError("Не удалось сжать видео для Telegram.")

    # Последняя попытка — жёсткий лимит размера (может обрезать хвост ролика).
    capped = out.with_name(f"{out.stem}_capped.mp4")
    cmd = [
        ffmpeg,
        "-y",
        "-i",
        str(src),
        "-vcodec",
        "libx264",
        "-crf",
        "36",
        "-preset",
        "faster",
        "-vf",
        "scale='min(640,iw)':-2",
        "-acodec",
        "aac",
        "-b:a",
        "96k",
        "-movflags",
        "+faststart",
        "-fs",
        str(limit),
        str(capped),
    ]
    logger.warning("Сжатие: обычные попытки не уложились в лимит — режим -fs (возможна обрезка).")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "")[-500:]
        raise RuntimeError(f"ffmpeg (-fs) ошибка: {err}")
    capped.replace(out)
    logger.info("Видео сжато с -fs: %.1f МБ → %.1f МБ", orig_mb, out.stat().st_size / (1024 * 1024))
    return out


def prepare_video_for_telegram(path: Path, *, dest: Path | None = None) -> Path:
    """Сжимает видео при необходимости; иначе возвращает исходный путь."""
    if not video_needs_compress(path):
        return path
    return compress_video_for_telegram(path, dest=dest)

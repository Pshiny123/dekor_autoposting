from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

YANDEX_ENDPOINT = "https://storage.yandexcloud.net"
DRIVE_ID_RE = re.compile(r"[?&]id=([^&]+)")


def bucket_name() -> str:
    return (os.getenv("YANDEX_BUCKET") or "dekorautoposting").strip()


def yandex_configured() -> bool:
    return bool(os.getenv("YANDEX_ACCESS_KEY_ID", "").strip() and os.getenv("YANDEX_SECRET_ACCESS_KEY", "").strip())


def yandex_public_url(key: str, bucket: str | None = None) -> str:
    b = bucket or bucket_name()
    return f"{YANDEX_ENDPOINT}/{b}/{key}"


def is_yandex_url(url: str, bucket: str | None = None) -> bool:
    b = (bucket or bucket_name()).lower()
    u = url.strip().lower()
    return "storage.yandexcloud.net" in u and f"/{b}/" in u


def yandex_url_to_key(url: str, bucket: str | None = None) -> str | None:
    b = bucket or bucket_name()
    prefix = f"{YANDEX_ENDPOINT}/{b}/"
    u = url.strip()
    if u.startswith(prefix):
        return u[len(prefix) :]
    return None


def is_drive_url(url: str) -> bool:
    return "drive.google.com" in url.lower()


def is_http_url(url: str) -> bool:
    u = url.strip().lower()
    return u.startswith("http://") or u.startswith("https://")


def drive_file_id(url: str) -> str | None:
    m = DRIVE_ID_RE.search(url)
    return m.group(1) if m else None


def guess_ext(url: str, kind: str, local_path: Path | None = None) -> str:
    if local_path is not None and local_path.suffix:
        return local_path.suffix.lstrip(".").lower()
    path = urlparse(url).path.lower()
    for ext in (".mp4", ".mov", ".webm", ".jpg", ".jpeg", ".png", ".webp"):
        if path.endswith(ext):
            return ext.lstrip(".")
    return "mp4" if kind == "video" else "jpg"


def object_key(post_id: str, col: str, ext: str) -> str:
    return f"posts/{post_id}/{col}.{ext}"


def cell_prefix(post_id: str, col: str) -> str:
    return f"posts/{post_id}/{col}."


def get_s3_client():
    import boto3
    from botocore.config import Config

    key_id = os.getenv("YANDEX_ACCESS_KEY_ID", "").strip()
    secret = os.getenv("YANDEX_SECRET_ACCESS_KEY", "").strip()
    if not key_id or not secret:
        raise RuntimeError("Yandex Object Storage: не заданы YANDEX_ACCESS_KEY_ID / YANDEX_SECRET_ACCESS_KEY.")
    return boto3.client(
        "s3",
        endpoint_url=YANDEX_ENDPOINT,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        region_name="ru-central1",
        config=Config(signature_version="s3v4"),
    )


def list_objects(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token = None
    while True:
        kwargs: dict = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for item in resp.get("Contents", []):
            keys.append(item["Key"])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def delete_keys(s3, bucket: str, keys: list[str]) -> None:
    for key in keys:
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info("Бакет: удалён %s", key)


def delete_cell(s3, bucket: str, post_id: str, col: str) -> None:
    prefix = cell_prefix(post_id, col)
    keys = list_objects(s3, bucket, prefix)
    if keys:
        delete_keys(s3, bucket, keys)


def content_type_for_ext(ext: str) -> str:
    ext = ext.lower().lstrip(".")
    if ext == "mp4":
        return "video/mp4"
    if ext in {"jpg", "jpeg"}:
        return "image/jpeg"
    if ext == "png":
        return "image/png"
    if ext == "webp":
        return "image/webp"
    return "application/octet-stream"


def upload_file(s3, bucket: str, local_path: Path, key: str) -> None:
    ext = local_path.suffix.lower().lstrip(".")
    s3.upload_file(
        str(local_path),
        bucket,
        key,
        ExtraArgs={"ACL": "public-read", "ContentType": content_type_for_ext(ext)},
    )


def object_size(s3, bucket: str, key: str) -> int | None:
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        return int(head["ContentLength"])
    except Exception:
        return None


def download_object(s3, bucket: str, key: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(dest))


def download_google_drive(file_id: str, dest: Path) -> None:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    base = "https://docs.google.com/uc?export=download"
    resp = session.get(base, params={"id": file_id}, stream=True, timeout=120)
    resp.raise_for_status()

    if "text/html" in (resp.headers.get("Content-Type") or "").lower():
        html = resp.text
        confirm = None
        m = re.search(r'confirm=([^&"]+)', html)
        if m:
            confirm = m.group(1)
        uuid_m = re.search(r'name="uuid"\s+value="([^"]+)"', html)
        if uuid_m:
            params = {"id": file_id, "export": "download", "confirm": confirm or "t", "uuid": uuid_m.group(1)}
            resp = session.get(
                "https://drive.usercontent.google.com/download",
                params=params,
                stream=True,
                timeout=600,
            )
        elif confirm:
            resp = session.get(base, params={"id": file_id, "confirm": confirm}, stream=True, timeout=600)
        else:
            raise RuntimeError(f"Google Drive вернул HTML вместо файла (id={file_id})")

    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    if dest.stat().st_size < 1024:
        head = dest.read_bytes()[:200]
        if b"<html" in head.lower():
            raise RuntimeError(f"Скачался HTML, не файл (id={file_id})")


def download_http(url: str, dest: Path) -> None:
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)


def download_media(url: str, dest: Path) -> None:
    if is_drive_url(url):
        fid = drive_file_id(url)
        if not fid:
            raise RuntimeError(f"Не удалось извлечь id из Drive URL: {url}")
        download_google_drive(fid, dest)
    elif is_http_url(url):
        download_http(url, dest)
    else:
        src = Path(url)
        if not src.is_file():
            src = Path.cwd() / url
        if not src.is_file():
            raise FileNotFoundError(f"Локальный файл не найден: {url}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(src.read_bytes())

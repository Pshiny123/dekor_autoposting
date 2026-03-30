from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from urllib.parse import quote, urlparse

import pandas as pd
try:
    import gspread
except Exception:  # pragma: no cover
    gspread = None  # type: ignore[assignment]


@dataclass(frozen=True)
class Post:
    post_id: str
    text: str
    photos: list[str]
    videos: list[str]


def _norm(s: str) -> str:
    return s.strip().casefold()


def _is_google_sheets_url(source: str) -> bool:
    return source.strip().lower().startswith("https://docs.google.com/spreadsheets/")


def _extract_gsheet_id(url: str) -> str:
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    try:
        i = parts.index("d")
        return parts[i + 1]
    except Exception as exc:
        raise ValueError("Не удалось извлечь spreadsheet id из Google Sheets URL.") from exc


def _get_gspread_client():
    if gspread is None:
        return None
    inline_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_INLINE", "").strip()
    if inline_json:
        try:
            info = json.loads(inline_json)
            return gspread.service_account_from_dict(info)
        except Exception as exc:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON_INLINE содержит невалидный JSON.") from exc
    creds_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if creds_path:
        p = Path(creds_path).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"Файл service account не найден: {p}")
        return gspread.service_account(filename=str(p))
    return None


def _read_gsheet_df(url: str, sheet_name: str) -> pd.DataFrame | None:
    sid = _extract_gsheet_id(url)
    client = _get_gspread_client()
    if client is not None:
        try:
            ws = client.open_by_key(sid).worksheet(sheet_name)
            values = ws.get_all_values()
            if not values:
                return pd.DataFrame()
            header = values[0]
            rows = values[1:] if len(values) > 1 else []
            return pd.DataFrame(rows, columns=header)
        except Exception:
            return None
    # fallback only for public sheets
    csv_url = f"https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv&sheet={quote(sheet_name)}"
    try:
        return pd.read_csv(csv_url)
    except Exception:
        return None


def _pick_sheet_name(source: str, desired: str) -> str:
    if _is_google_sheets_url(source):
        candidates: list[str] = []
        if desired:
            candidates.extend([desired, desired.lower(), desired.capitalize()])
        candidates.extend(["Posts", "posts"])
        seen: set[str] = set()
        for c in candidates:
            if c in seen:
                continue
            seen.add(c)
            if _read_gsheet_df(source, c) is not None:
                return c
        raise ValueError("Не удалось прочитать лист Posts из Google Sheets.")

    xlsx_path = Path(source)
    xls = pd.ExcelFile(xlsx_path)
    if not desired:
        return xls.sheet_names[0]

    desired_norm = _norm(desired)
    for name in xls.sheet_names:
        if _norm(name) == desired_norm:
            return name
    # fallback: common name used in вашей задаче
    for name in xls.sheet_names:
        if _norm(name) == "posts":
            return name
    raise ValueError(
        f"Лист '{desired}' не найден. Доступные листы: {xls.sheet_names}"
    )


def load_posts(source: str | Path, sheet_name: str) -> list[Post]:
    source_s = str(source).strip()
    if _is_google_sheets_url(source_s):
        actual_sheet = _pick_sheet_name(source_s, sheet_name)
        df = _read_gsheet_df(source_s, actual_sheet)
        if df is None:
            raise ValueError("Не удалось прочитать лист Posts из Google Sheets.")
    else:
        xlsx_path = Path(source_s).resolve()
        if not xlsx_path.exists():
            raise FileNotFoundError(f"Excel файл не найден: {xlsx_path}")
        actual_sheet = _pick_sheet_name(str(xlsx_path), sheet_name)
        df = pd.read_excel(xlsx_path, sheet_name=actual_sheet)

    if "text" not in df.columns:
        raise ValueError("В Excel листе должна быть колонка 'text'.")
    if "ID" not in df.columns:
        raise ValueError("В Excel листе должна быть колонка 'ID'.")

    posts: list[Post] = []
    for _, row in df.iterrows():
        raw_id = row.get("ID", "")
        post_id = "" if pd.isna(raw_id) else str(raw_id).strip()
        raw_text = row.get("text", "")
        text = "" if pd.isna(raw_text) else str(raw_text)

        photos: list[str] = []
        videos: list[str] = []
        for i in range(1, 11):
            p = row.get(f"Photo{i}", "")
            v = row.get(f"Video{i}", "")
            if p is not None and not pd.isna(p) and str(p).strip():
                photos.append(str(p).strip())
            if v is not None and not pd.isna(v) and str(v).strip():
                videos.append(str(v).strip())

        # пропускаем полностью пустые строки
        if (not post_id) and (not text.strip()) and (not photos) and (not videos):
            continue

        if not post_id:
            # если ID пустой, всё равно добавим, но по нему нельзя будет адресовать из Queue
            post_id = str(len(posts) + 1)
        posts.append(Post(post_id=post_id, text=text, photos=photos, videos=videos))

    if not posts:
        raise ValueError("Не найдено ни одного поста (проверьте колонку text и строки).")
    return posts


def index_posts_by_id(posts: list[Post]) -> dict[str, Post]:
    out: dict[str, Post] = {}
    for p in posts:
        out[p.post_id] = p
    return out


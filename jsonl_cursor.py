#!/usr/bin/env python3
"""
Persistent JSONL Cursor Utility

Purpose:
- Prevent engines from missing JSONL records after restart.
- Prevent engines from re-reading huge files into RAM.
- Store inode + byte offset + last_ts + last_id per consumer/source.
- Detect file rotation/truncation.
- Provide tail iteration with bounded memory.

This is infrastructure, not a new trading engine.
"""

import os
import json
import time
from pathlib import Path
from typing import Iterator, Optional, Dict, Any

CURSOR_DIR = Path("data/cursors")
CURSOR_DIR.mkdir(parents=True, exist_ok=True)


def _safe_name(value: str) -> str:
    return (
        value.replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace(" ", "_")
    )


def cursor_path(consumer: str, source: str) -> Path:
    return CURSOR_DIR / f"{_safe_name(consumer)}__{_safe_name(source)}.json"


def file_identity(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"exists": False, "inode": None, "size": 0, "mtime": None}
    st = path.stat()
    return {
        "exists": True,
        "inode": st.st_ino,
        "size": st.st_size,
        "mtime": st.st_mtime,
    }


def load_cursor(consumer: str, source: str) -> Dict[str, Any]:
    p = cursor_path(consumer, source)
    if not p.exists():
        return {
            "consumer": consumer,
            "source": source,
            "inode": None,
            "offset": 0,
            "last_ts": None,
            "last_id": None,
            "updated_at": None,
        }
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {
            "consumer": consumer,
            "source": source,
            "inode": None,
            "offset": 0,
            "last_ts": None,
            "last_id": None,
            "updated_at": None,
            "corrupt_cursor_recovered": True,
        }


def save_cursor(
    consumer: str,
    source: str,
    inode: int | None,
    offset: int,
    last_ts: int | None = None,
    last_id: str | None = None,
) -> None:
    p = cursor_path(consumer, source)
    tmp = p.with_suffix(".tmp")
    payload = {
        "consumer": consumer,
        "source": source,
        "inode": inode,
        "offset": offset,
        "last_ts": last_ts,
        "last_id": last_id,
        "updated_at": time.time(),
    }
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(p)


def _extract_ts_and_id(row: Dict[str, Any]) -> tuple[int | None, str | None]:
    ts = (
        row.get("window_start_ts")
        or row.get("qualification_ts")
        or row.get("ts")
        or row.get("opened_ts")
        or row.get("created_ts")
    )
    rid = (
        row.get("setup_id")
        or row.get("qualified_setup_id")
        or row.get("source_setup_id")
        or row.get("trade_id")
        or row.get("observation_id")
        or row.get("event_id")
    )
    try:
        ts = int(ts) if ts is not None else None
    except Exception:
        ts = None
    return ts, str(rid) if rid is not None else None


def iter_new_jsonl(
    path: str | Path,
    consumer: str,
    source_name: Optional[str] = None,
    start_mode: str = "resume",
    max_lines: Optional[int] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Iterate only new JSONL rows from persistent cursor.

    start_mode:
      - resume: continue from saved offset, or 0 if no cursor exists
      - eof_if_new: if no cursor exists, start from EOF
      - beginning_if_new: if no cursor exists, start from 0

    Rotation/truncation:
      - inode changed or file size < saved offset => reset offset.
      - default reset offset is 0, to avoid missing records.
    """
    p = Path(path)
    source = source_name or str(p)
    ident = file_identity(p)
    if not ident["exists"]:
        return

    cur = load_cursor(consumer, source)
    inode = ident["inode"]
    size = ident["size"]

    saved_inode = cur.get("inode")
    offset = int(cur.get("offset") or 0)

    if saved_inode is None:
        if start_mode == "eof_if_new":
            offset = size
        else:
            offset = 0
    elif saved_inode != inode or size < offset:
        offset = 0

    count = 0
    last_ts = cur.get("last_ts")
    last_id = cur.get("last_id")

    with p.open("r", encoding="utf-8", errors="replace") as fh:
        fh.seek(offset)
        while True:
            pos_before = fh.tell()
            line = fh.readline()
            if not line:
                save_cursor(consumer, source, inode, fh.tell(), last_ts, last_id)
                break

            stripped = line.strip()
            if not stripped:
                save_cursor(consumer, source, inode, fh.tell(), last_ts, last_id)
                continue

            try:
                row = json.loads(stripped)
            except Exception:
                save_cursor(consumer, source, inode, fh.tell(), last_ts, last_id)
                continue

            ts, rid = _extract_ts_and_id(row)
            if ts is not None:
                last_ts = ts
            if rid is not None:
                last_id = rid

            save_cursor(consumer, source, inode, fh.tell(), last_ts, last_id)
            yield row

            count += 1
            if max_lines is not None and count >= max_lines:
                save_cursor(consumer, source, inode, fh.tell(), last_ts, last_id)
                break

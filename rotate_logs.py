"""
NurtacCoreEngineClaude — Log Rotation Script

Moves data/*.jsonl files to data/archive/YYYY-MM-DD/ and compresses
archives older than 30 days into .tar.gz files.

Because the engine files use open-per-write (open + close on every append),
rotation is safe to run while engines are running — the engines will simply
create new empty files on their next write.

Usage:
  python3 rotate_logs.py          # rotate today's logs

Crontab example (UTC midnight daily):
  0 0 * * * cd /root/NurtacCoreEngineClaude && python3 rotate_logs.py

NOTE: If you run engines that keep file handles open across writes, restart
the engines after rotation so they create fresh file handles pointing to the
new (empty) files. The current engines reopen the file on every write, so
restart is NOT required.
"""

import glob
import os
import shutil
import tarfile
import time
from datetime import datetime, timezone

DATA_DIR    = "data"
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive")
KEEP_DAYS   = 30   # compress archives older than this many days


def _today_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def rotate_today() -> None:
    label      = _today_label()
    target_dir = os.path.join(ARCHIVE_DIR, label)
    os.makedirs(target_dir, exist_ok=True)

    moved = []
    for src in glob.glob(os.path.join(DATA_DIR, "*.jsonl")):
        dst = os.path.join(target_dir, os.path.basename(src))
        shutil.move(src, dst)
        moved.append(os.path.basename(src))

    if moved:
        print(f"[ROTATE] Moved {len(moved)} file(s) to {target_dir}: {moved}")
    else:
        print(f"[ROTATE] No .jsonl files found in {DATA_DIR}/ — nothing to rotate.")


def compress_old_archives() -> None:
    if not os.path.isdir(ARCHIVE_DIR):
        return

    cutoff = time.time() - KEEP_DAYS * 86_400
    for entry in os.scandir(ARCHIVE_DIR):
        if not entry.is_dir():
            continue
        # Directory names are YYYY-MM-DD
        try:
            dt  = datetime.strptime(entry.name, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            age = time.time() - dt.timestamp()
        except ValueError:
            continue

        if age <= KEEP_DAYS * 86_400:
            continue

        tar_path = entry.path + ".tar.gz"
        if os.path.exists(tar_path):
            continue  # already compressed

        print(f"[ROTATE] Compressing old archive: {entry.path} → {tar_path}")
        try:
            with tarfile.open(tar_path, "w:gz") as tar:
                tar.add(entry.path, arcname=entry.name)
            shutil.rmtree(entry.path)
            print(f"[ROTATE] Compressed and removed: {entry.path}")
        except Exception as exc:
            print(f"[ROTATE] Failed to compress {entry.path}: {exc}")


def main() -> None:
    print("NurtacCoreEngineClaude — Log Rotation")
    rotate_today()
    compress_old_archives()
    print("[ROTATE] Done.")


if __name__ == "__main__":
    main()

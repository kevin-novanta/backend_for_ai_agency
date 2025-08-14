from __future__ import annotations
import json
from .paths import OFFSETS_PATH, LOCK_DIR
from .file_lock import file_lock

def get_offset(inbox: str) -> int:
    if not OFFSETS_PATH.exists():
        return 0
    data = json.loads(OFFSETS_PATH.read_text())
    return int(data.get(inbox, 0))

def set_offset(inbox: str, value: int) -> None:
    data = {}
    if OFFSETS_PATH.exists():
        data = json.loads(OFFSETS_PATH.read_text())
    data[inbox] = int(value)
    lock_path = str(LOCK_DIR / "offsets.lock")
    with file_lock(lock_path):
        OFFSETS_PATH.write_text(json.dumps(data, indent=2))
from __future__ import annotations
from contextlib import contextmanager
import os, time

@contextmanager
def file_lock(lock_path: str, retries: int = 40, delay: float = 0.05):
    # simple atomic lock via mkdir (works on mac/linux)
    for _ in range(retries):
        try:
            os.mkdir(lock_path)
            break
        except FileExistsError:
            time.sleep(delay)
    try:
        yield
    finally:
        try:
            os.rmdir(lock_path)
        except FileNotFoundError:
            pass
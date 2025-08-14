from __future__ import annotations
import logging
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = REPO_ROOT / "workflows" / "followup_engine" / "gmail_watch" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OFFSETS_PATH = DATA_DIR / "gmail_offsets.json"
LOCK_DIR = DATA_DIR / ".locks"
LOCK_DIR.mkdir(exist_ok=True)

# use your existing logger if you have one in followup_engine.utils.logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("gmail_watch")
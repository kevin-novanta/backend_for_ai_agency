from __future__ import annotations
import logging
from pathlib import Path

_LOGGERS = {}


def get_logger(name: str = "followup") -> logging.Logger:
    if name in _LOGGERS:
        return _LOGGERS[name]

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(ch)

    # File handler (logs/followup_run.log)
    root = Path(__file__).resolve().parents[3]
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_dir / "followup_run.log")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(fh)

    _LOGGERS[name] = logger
    return logger

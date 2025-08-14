from __future__ import annotations
from pathlib import Path
import yaml

CONFIG_PATH = Path(__file__).parents[1] / "config" / "sequences.yml"


def load_sequences_cfg(path: Path | None = None):
    """Load the YAML config for sequences. Returns a dict.
    Default path: workflows/followup_engine/config/sequences.yml
    """
    cfg_path = path or CONFIG_PATH
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)



from pathlib import Path
import json

_ROOT = Path(__file__).resolve().parents[2]  # workflows/followup_engine
_SETTINGS = _ROOT / "settings" / "llm.json"

_def_cfg = {"provider": "openai", "model": "gpt-4o-mini", "temperature": 0.6, "max_tokens": 600}


def load_llm_cfg() -> dict:
    try:
        return json.loads(_SETTINGS.read_text())
    except Exception:
        return dict(_def_cfg)

# Example stub for callers to access model settings; actual API call is elsewhere.
class LLMConfig:
    def __init__(self):
        self.cfg = load_llm_cfg()

    @property
    def provider(self) -> str:
        return self.cfg.get("provider", _def_cfg["provider"]) 

    @property
    def model(self) -> str:
        return self.cfg.get("model", _def_cfg["model"]) 

    @property
    def temperature(self) -> float:
        return float(self.cfg.get("temperature", _def_cfg["temperature"]))

    @property
    def max_tokens(self) -> int:
        return int(self.cfg.get("max_tokens", _def_cfg["max_tokens"]))
from __future__ import annotations
from typing import List, Any

def poll_ids(gc: Any, inbox: str, since_epoch_ms: int, lookback_minutes: int = 1440) -> List[str]:
    """Return message IDs newer than watermark for this inbox. Stub for now."""
    # When wired, build a Gmail query like: f"in:inbox newer_than:{lookback_minutes}m -from:{inbox}"
    # Then call gc.search_ids(query) and filter by internalDate > since_epoch_ms
    return []
from __future__ import annotations
import sqlite3, threading
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime

# One lightweight DB file shared by all pipelines
_DB_DIR = Path(__file__).parent
_DB_PATH = _DB_DIR / "followup_state.sqlite3"
_LOCK = threading.Lock()

_DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS lead_state (
  lead_id TEXT NOT NULL,
  sequence_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE|PAUSED|STOPPED|DONE|REPLIED
  responded INTEGER NOT NULL DEFAULT 0,
  stop_all INTEGER NOT NULL DEFAULT 0,
  current_step TEXT,
  next_action_at TEXT,
  last_event_at TEXT,
  updated_at TEXT,
  PRIMARY KEY (lead_id, sequence_id)
);

CREATE TABLE IF NOT EXISTS processed_events (
  provider TEXT NOT NULL,
  event_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (provider, event_id)
);

-- Per-step idempotency: if the exact same step+body hash was already sent, skip.
CREATE TABLE IF NOT EXISTS sent_steps (
  lead_id TEXT NOT NULL,
  sequence_id TEXT NOT NULL,
  step_id TEXT NOT NULL,
  idem TEXT NOT NULL,              -- hash of (lead_id|step_id|body) or provider msg id
  sent_at TEXT NOT NULL,
  PRIMARY KEY (lead_id, sequence_id, step_id, idem)
);

CREATE INDEX IF NOT EXISTS ix_lead_state_status ON lead_state(status, responded, stop_all);
"""

class StateStore:
    """
    Central state + idempotency for all sequences.
    - lead_state rows are keyed by (lead_id, sequence_id)
    - There's also a synthetic row with sequence_id='__all__' used for global flags
      (e.g., replied/stop_all that should stop every pipeline for the lead).
    """
    def __init__(self, client: str, db_path: Path | None = None):
        self.client = client
        self.db_path = Path(db_path) if db_path else _DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as c:
            c.executescript(_DDL)

    # ---------- low-level ----------
    def _conn(self):
        return sqlite3.connect(self.db_path)

    def _iso(self) -> str:
        return datetime.utcnow().isoformat()

    # ---------- global stop / replied ----------
    def should_stop_all(self, lead_id: str) -> bool:
        """
        True if this lead is globally stopped (replied, stopped, done).
        Checked before every step to avoid any send.
        """
        with self._conn() as c:
            row = c.execute(
                "SELECT stop_all, responded, status FROM lead_state WHERE lead_id=? AND sequence_id='__all__' LIMIT 1",
                (lead_id,)
            ).fetchone()
        if not row:
            return False
        stop_all, responded, status = row
        return bool(stop_all) or bool(responded) or status in ("REPLIED", "STOPPED", "DONE")

    def mark_replied(self, lead_id: str) -> None:
        """
        When a webhook/poller detects a real reply:
        - mark global row (__all__) as REPLIED + stop_all
        - cascade stop_all to any existing sequence rows for this lead
        """
        now = self._iso()
        with self._conn() as c, _LOCK:
            c.execute("""
              INSERT INTO lead_state(lead_id, sequence_id, status, responded, stop_all, updated_at)
              VALUES(?, '__all__', 'REPLIED', 1, 1, ?)
              ON CONFLICT(lead_id, sequence_id) DO UPDATE SET
                status='REPLIED', responded=1, stop_all=1, updated_at=excluded.updated_at
            """, (lead_id, now))
            c.execute("UPDATE lead_state SET status='REPLIED', responded=1, stop_all=1, updated_at=? WHERE lead_id=?",
                      (now, lead_id))

    def set_global_status(self, lead_id: str, status: str) -> None:
        now = self._iso()
        stop_all = 1 if status in ("REPLIED", "STOPPED", "DONE") else 0
        with self._conn() as c, _LOCK:
            c.execute("""
              INSERT INTO lead_state(lead_id, sequence_id, status, stop_all, updated_at)
              VALUES(?, '__all__', ?, ?, ?)
              ON CONFLICT(lead_id, sequence_id) DO UPDATE SET
                status=excluded.status, stop_all=excluded.stop_all, updated_at=excluded.updated_at
            """, (lead_id, status, stop_all, now))

    # ---------- webhook/poller event idempotency ----------
    def event_seen(self, provider: str, event_id: str) -> bool:
        """
        Returns True if we've already processed this webhook/poller event.
        """
        with self._conn() as c, _LOCK:
            row = c.execute("SELECT 1 FROM processed_events WHERE provider=? AND event_id=?",
                            (provider, event_id)).fetchone()
            if row:
                return True
            c.execute("INSERT INTO processed_events(provider, event_id, created_at) VALUES(?,?,?)",
                      (provider, event_id, self._iso()))
            return False

    # ---------- sequence pointers ----------
    def get_pointer(self, lead_id: str, sequence_id: str) -> Tuple[Optional[str], Optional[str], str]:
        """
        Returns (current_step, next_action_at, status) for this lead+sequence.
        If never seen, treat as ACTIVE with no pointer.
        """
        with self._conn() as c:
            row = c.execute("""
              SELECT current_step, next_action_at, status
              FROM lead_state WHERE lead_id=? AND sequence_id=?
            """, (lead_id, sequence_id)).fetchone()
        return row or (None, None, "ACTIVE")

    def should_run(self, lead_id: str, sequence_id: str, step_id: str, now: datetime) -> bool:
        """
        Gate a step before running:
        - stop if global stop_all/replied
        - otherwise you can also consult next_action_at if you set scheduling
        """
        if self.should_stop_all(lead_id):
            return False
        # (Optional) add time-based gating here using next_action_at
        return True

    def advance(self, lead_id: str, sequence_id: str, step_id: str, result: dict) -> None:
        """
        Move the pointer forward and schedule the next action if provided.
        """
        next_action_at = result.get("next_action_at")
        if next_action_at and hasattr(next_action_at, "isoformat"):
            next_action_at = next_action_at.isoformat()
        now = self._iso()
        with self._conn() as c, _LOCK:
            c.execute("""
              INSERT INTO lead_state(lead_id, sequence_id, status, current_step, next_action_at, updated_at)
              VALUES(?,?,?,?,?,?)
              ON CONFLICT(lead_id, sequence_id) DO UPDATE SET
                current_step=excluded.current_step,
                next_action_at=excluded.next_action_at,
                updated_at=excluded.updated_at
            """, (lead_id, sequence_id, "ACTIVE", step_id, next_action_at, now))

    # ---------- per-step idempotency ----------
    def was_sent(self, lead_id: str, sequence_id: str, step_id: str, idem: str) -> bool:
        with self._conn() as c:
            row = c.execute("""
              SELECT 1 FROM sent_steps WHERE lead_id=? AND sequence_id=? AND step_id=? AND idem=?
            """, (lead_id, sequence_id, step_id, idem)).fetchone()
        return bool(row)

    def mark_sent(self, lead_id: str, sequence_id: str, step_id: str, idem: str) -> None:
        with self._conn() as c, _LOCK:
            c.execute("""
              INSERT OR IGNORE INTO sent_steps(lead_id, sequence_id, step_id, idem, sent_at)
              VALUES(?,?,?,?,?)
            """, (lead_id, sequence_id, step_id, idem, self._iso()))
            # Also keep the sequence pointer current
            c.execute("""
              INSERT INTO lead_state(lead_id, sequence_id, status, current_step, updated_at)
              VALUES(?,?,?,?,?)
              ON CONFLICT(lead_id, sequence_id) DO UPDATE SET
                current_step=excluded.current_step,
                updated_at=excluded.updated_at
            """, (lead_id, sequence_id, "ACTIVE", step_id, self._iso()))
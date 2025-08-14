# preflight.py — lead pre-send filtering & verification pipeline
# This module centralizes: client gating, status checks, ZeroBounce verification (with caching),
# and a Deliverability allow-list. It returns (eligible_rows, skip_logs, settings_logs)
# so the caller (sequence_runner) can log what happened.

from __future__ import annotations
from typing import List, Dict, Tuple

import os
import json
from datetime import datetime
from pathlib import Path

import requests

# -----------------------------------------------------------------------------
# Config / constants
# -----------------------------------------------------------------------------
UTILS_DIR = Path(__file__).parent
VERIFY_CACHE_PATH = UTILS_DIR / "email_verify_cache.json"
# We also support a repo-level creds file if ENV is not set
REPO_ROOT = UTILS_DIR.parents[2] if len(UTILS_DIR.parents) >= 2 else UTILS_DIR
ZB_CREDS_PATH = REPO_ROOT / "Creds" / "zerobounce_key.json"


# -----------------------------------------------------------------------------
# ZeroBounce helpers (self-contained)
# -----------------------------------------------------------------------------

def _today_str() -> str:
    return datetime.today().strftime("%Y-%m-%d")


def _load_zb_key_and_cache_days() -> Tuple[str, int]:
    """Load ZeroBounce API key and default cache_days.
    Priority: ENV var ZB_API_KEY > Creds/zerobounce_key.json > defaults.
    Returns: (api_key, cache_days)
    """
    key = (os.getenv("ZB_API_KEY") or "").strip()
    if key:
        return key, 14
    try:
        if ZB_CREDS_PATH.exists():
            data = json.load(open(ZB_CREDS_PATH, "r", encoding="utf-8"))
            return (data.get("ZB_API_KEY", ""), int(data.get("cache_days", 14)))
    except Exception:
        pass
    return "", 14


def _load_verify_cache() -> Dict[str, Dict]:
    try:
        if VERIFY_CACHE_PATH.exists():
            return json.load(open(VERIFY_CACHE_PATH, "r", encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_verify_cache(cache: Dict[str, Dict]) -> None:
    try:
        json.dump(cache, open(VERIFY_CACHE_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    except Exception:
        pass


def verify_with_zerobounce(email: str, cache_days: int = 14) -> Dict[str, str]:
    """
    Call ZeroBounce (with local JSON caching) and normalize outputs to your CRM.
    Returns dict with keys:
      - status: deliverable|undeliverable|risky|catch-all|unknown
      - reason: provider sub-status string
      - date: YYYY-MM-DD (verification date)
      - deliverability: Safe|Catch All|Risky (mapped to your CRM dropdown)
    """
    email = (email or "").strip()
    if not email or "@" not in email:
        return {"status": "unknown", "reason": "bad_format", "date": _today_str(), "deliverability": "Risky"}

    cache = _load_verify_cache()
    cached = cache.get(email.lower())
    if cached:
        try:
            d = datetime.strptime(cached.get("date", ""), "%Y-%m-%d")
            if (datetime.today() - d).days <= cache_days:
                return cached
        except Exception:
            pass

    api_key, default_cache_days = _load_zb_key_and_cache_days()
    cache_days = cache_days or default_cache_days
    if not api_key:
        return {"status": "unknown", "reason": "no_api_key", "date": _today_str(), "deliverability": "Risky"}

    try:
        r = requests.get(
            "https://api.zerobounce.net/v2/validate",
            params={"api_key": api_key, "email": email},
            timeout=12,
        )
        data = r.json() if r.ok else {}
        raw = (data.get("status") or "").lower()  # valid, invalid, catch-all, unknown, spamtrap, abuse, do_not_mail
        sub = (data.get("sub_status") or "").replace("_", " ")

        if raw == "valid":
            deliverability = "Safe"; norm = "deliverable"
        elif raw == "catch-all":
            deliverability = "Catch All"; norm = "catch-all"
        elif raw in {"invalid"}:
            deliverability = "Risky"; norm = "undeliverable"
        elif raw in {"spamtrap", "abuse", "do_not_mail"}:
            deliverability = "Risky"; norm = raw
        else:
            deliverability = "Risky"; norm = raw or "unknown"

        result = {"status": norm, "reason": sub, "date": _today_str(), "deliverability": deliverability}
        cache[email.lower()] = result
        _save_verify_cache(cache)
        return result
    except Exception as e:
        return {"status": "unknown", "reason": f"verify_error:{type(e).__name__}", "date": _today_str(), "deliverability": "Risky"}


# -----------------------------------------------------------------------------
# Public preflight filter
# -----------------------------------------------------------------------------

def preflight_filter(
    rows: List[Dict],
    controls: Dict,
    client_col_name: str,
    selected_client_norm: str,
) -> Tuple[List[Dict], List[str], List[str]]:
    """
    Apply gating logic before sending. Returns (eligible_rows, skip_logs, settings_logs).

    Logic included:
      - Client match / basic status checks
      - ZeroBounce verification (optional, with caching) → writes Deliverability
      - Hard block on provider statuses (controls.verification.block_statuses)
      - Deliverability allow-list (controls.allowed_deliverability_statuses)
    """
    print(f"Starting preflight_filter with {len(rows)} rows for client '{selected_client_norm}'")
    skip_logs: List[str] = []
    settings_logs: List[str] = []
    eligible: List[Dict] = []

    # Controls
    use_deliv_filter = bool(controls.get("use_deliverability_filter", False))
    allowed_deliv = list(controls.get("allowed_deliverability_statuses", [])) or []
    # Normalize allowed deliverability values for case/whitespace-insensitive comparison
    allowed_deliv_norm = [str(a).strip().lower() for a in allowed_deliv]

    print(f"Deliverability filter enabled: {use_deliv_filter}")
    print(f"Allowed deliverability statuses: {allowed_deliv}")

    vcfg = controls.get("verification") or {}
    verif_enabled = bool(vcfg.get("enabled", False))
    verif_provider = (vcfg.get("provider") or "").lower()
    verif_cache_days = int(vcfg.get("cache_days", 14))
    verif_block_statuses = set(s.lower() for s in (vcfg.get("block_statuses") or []))

    print(f"Verification enabled: {verif_enabled}")
    print(f"Verification provider: '{verif_provider}'")
    print(f"Verification cache days: {verif_cache_days}")
    print(f"Verification block statuses: {sorted(verif_block_statuses)}")

    # Settings logs (for the caller to print)
    if use_deliv_filter:
        settings_logs.append(f"Deliverability filter ON. Allowed statuses: {allowed_deliv}")
    else:
        settings_logs.append("Deliverability filter OFF.")

    if verif_enabled and verif_provider == "zerobounce":
        settings_logs.append(
            f"Verification ON via ZeroBounce (cache_days={verif_cache_days}). Block statuses: {sorted(verif_block_statuses)}"
        )
    else:
        settings_logs.append("Verification OFF.")

    # Row processing
    for idx, row in enumerate(rows, start=1):
        print(f"\nProcessing row {idx}:")
        status = (row.get("Messaging Status") or "").strip().lower()
        row_client = " ".join((row.get(client_col_name) or "").split()).lower()
        sequence = (row.get("Sequence Stage") or "").strip().lower()
        responded = (row.get("Responded?") or "").strip().lower()

        print(f"  Client: '{row_client}', Sequence Stage: '{sequence}', Responded?: '{responded}', Messaging Status: '{status}'")

        # Basic gates
        if row_client != selected_client_norm:
            print(f"  Skipping due to client mismatch: row client '{row_client}' != selected '{selected_client_norm}'")
            continue
        if sequence:  # already in a sequence stage
            print(f"  Skipping due to existing sequence stage: '{sequence}'")
            continue
        if responded == "yes":
            print("  Skipping because lead has already responded")
            continue
        if status not in ("", "untouched", "new"):
            print(f"  Skipping due to messaging status '{status}' not in allowed set ('', 'untouched', 'new')")
            continue

        # --- Verification (ZeroBounce) ---
        if verif_enabled and verif_provider == "zerobounce":
            if (row.get("Deliverability") or "").strip():
                # Deliverability already set, skip API call
                print("  Deliverability already set; skipping ZeroBounce API call")
                v = {
                    "status": "",
                    "reason": "",
                    "date": _today_str(),
                    "deliverability": row.get("Deliverability").strip()
                }
            else:
                print(f"  Calling ZeroBounce API for email: {row.get('Email')}")
                v = verify_with_zerobounce(row.get("Email"), cache_days=verif_cache_days)
            # Persist mapped Deliverability in-memory so allow-list can act on it
            if v.get("deliverability"):
                row["Deliverability"] = v["deliverability"]
            # Optional audit fields (caller can persist to CSV if desired)
            row["Email Verification Status"] = v.get("status", "")
            row["Email Verification Reason"] = v.get("reason", "")
            row["Last Verified Date"] = v.get("date", "")

            print(f"  Verification result: status='{v.get('status','')}', reason='{v.get('reason','')}', deliverability='{v.get('deliverability','')}'")

            v_status = (v.get("status") or "").lower()
            if v_status in verif_block_statuses:
                log_msg = f"⛔ {row.get('Email')} blocked by verifier: {v_status} ({v.get('reason','')})"
                print(f"  {log_msg}")
                skip_logs.append(log_msg)
                continue
        # --- End Verification ---

        # --- Default blank deliverability to Safe ---
        deliverability_raw = (row.get("Deliverability") or "").strip()
        if not deliverability_raw:
            deliverability_raw = "Safe"
            row["Deliverability"] = "Safe"
            print("  Deliverability was blank, defaulted to 'Safe'")

        # Canonicalize common variants from CSV (case-insensitive)
        d_norm = deliverability_raw.lower()
        canon_map = {
            "safe": "Safe",
            "valid": "Safe",
            "catch-all": "Catch All",
            "catch all": "Catch All",
            "risky": "Risky",
            "invalid": "Risky",
            "undeliverable": "Risky",
            "unknown": "Unknown",
        }
        deliverability = canon_map.get(d_norm, deliverability_raw)  # preserve original if unknown
        # Write back the canonicalized value so downstream logs/CSV are consistent
        row["Deliverability"] = deliverability
        print(f"  Canonicalized Deliverability: '{deliverability}'")

        # --- Deliverability allow-list (case-insensitive) ---
        if use_deliv_filter:
            deliverability_norm = deliverability.lower()
            if allowed_deliv_norm and deliverability_norm not in allowed_deliv_norm:
                log_msg = f"⛔ {row.get('Email')} skipped: Deliverability='{deliverability}' not in {allowed_deliv}"
                print(f"  {log_msg}")
                skip_logs.append(log_msg)
                continue
            else:
                print(f"  Lead passes deliverability filter: '{deliverability}'")
        # --- End allow-list ---

        eligible.append(row)

    print(f"\nPreflight filtering complete. Eligible leads: {len(eligible)}, Skipped leads: {len(skip_logs)}")
    return eligible, skip_logs, settings_logs

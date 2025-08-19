from engine.subscripts.utils.crm_helpers import get

def resolve_owner_inbox(row: dict, fields_map: dict) -> str | None:
    """
    Return the assigned inbox from 'Owner / Assigned To'.
    If blank, return None so caller can SKIP this lead.
    """
    owner_col = fields_map.get("canonical", {}).get("owner", "Owner / Assigned To")
    owner = (get(row, owner_col) or "").strip()
    if not owner:
        return None
    return owner
from engine.subscripts.utils.crm_helpers import get, setf

def is_replied(row: dict, fields_map: dict) -> bool:
    """
    Return True if the lead has replied and set Messaging Status to 'Paused'.
    We honor either 'Responded?' or 'Replied?' being 'Yes' (case-insensitive).
    """
    can = fields_map.get("canonical", {})
    print(f"Canonical field mappings: {can}")
    responded_col = can.get("responded_flag", "Responded?")
    replied_col = can.get("replied_flag", "Replied?")
    msg_status_col = can.get("messaging_status", "Messaging Status")

    responded = (get(row, responded_col) or "").strip().lower()
    replied = (get(row, replied_col) or "").strip().lower()
    print(f"Responded value: '{responded}', Replied value: '{replied}'")

    has_replied = responded == "yes" or replied == "yes"
    if has_replied:
        setf(row, msg_status_col, "Paused")
        print(f"Lead has replied. Set '{msg_status_col}' to 'Paused'.")
    else:
        print("Lead has not replied.")
    return has_replied
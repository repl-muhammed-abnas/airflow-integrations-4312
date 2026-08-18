def get_user_uri(response):
    if not response:
        return None
    if isinstance(response, list):
        return response[0].get("uri") if response[0] else None
    return response.get("uri")

def first_or_none(response):
    if not response:
        return []
    if isinstance(response, list):
        for item in response:
            if item:
                return item
        return []
    return response

def filter_active_policies(response):
    """GetUserTimeOffTypePolicySummary returns the `d` value as an object with
    the per-type policies nested under 'policiesByTimeOffType'. Return the
    entries where time off is currently allowed (enabled), preserving the raw
    entry (so downstream can read timeOffType.uri) and adding flat
    timeofftypeuri/time_off_type_uri keys for the conf builders."""
    if not response:
        return []
    policies = response.get("policiesByTimeOffType") if isinstance(response, dict) else response
    if not isinstance(policies, list):
        return []
    out = []
    for p in policies:
        if not isinstance(p, dict):
            continue
        v = p.get("isTimeOffAllowedAgainstThisTimeOffType")
        if str(v).strip().lower() != "true":
            continue
        uri = (p.get("timeOffType") or {}).get("uri")
        out.append({**p, "timeofftypeuri": uri, "time_off_type_uri": uri})
    return out


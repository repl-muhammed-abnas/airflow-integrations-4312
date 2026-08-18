import rail
from datetime import datetime

null = None

def parse_replicon_date(value: str):
    """
    STRICT: ONLY DDMMYYYY allowed.
    Returns rail.parse_date(YYYY-MM-DD).
    """
    if not value:
        return None

    s = str(value).strip()
    if not (len(s) == 8 and s.isdigit()):
        return None

    try:
        dd = int(s[0:2])
        mm = int(s[2:4])
        yyyy = int(s[4:8])
        dt = datetime(yyyy, mm, dd)
        iso = dt.date().isoformat()
        return rail.parse_date(iso, "%Y-%m-%d")
    except:
        return None

def _to_dt_ddmmyyyy(value: str):
    """
    STRICT: parse DDMMYYYY → datetime or None.
    """
    if not value:
        return None

    s = str(value).strip()
    if not (len(s) == 8 and s.isdigit()):
        return None

    try:
        dd = int(s[0:2])
        mm = int(s[2:4])
        yyyy = int(s[4:8])
        return datetime(yyyy, mm, dd)
    except:
        return None


def validate_grant_row(row):
    """
    Validates a single grant row.
    End date is optional.
    Adds:
        is_valid: True/False
        validation_errors: semicolon-separated reasons
    """
    if not row:
        return [] 
    errors = []

    grant_name = row["grant_name"]
    grant_code = row["grant_code"]
    grant_status = row["grant_status"]

    start_raw = row["grant_start_date"]
    end_raw   = row["grant_end_date"]


    if not grant_code:
        errors.append("grant_code is mandatory.")

    if not grant_name:
        errors.append("grant_name is mandatory.")

    if grant_status not in ("0", "1"):
        errors.append("grant_status must be 0 or 1.")

    if not start_raw:
        errors.append("grant_start_date is mandatory.")

    start_date = parse_replicon_date(start_raw) if start_raw else None
    end_date   = parse_replicon_date(end_raw) if end_raw else None

    start_dt = _to_dt_ddmmyyyy(start_raw) if start_raw else None
    end_dt   = _to_dt_ddmmyyyy(end_raw) if end_raw else None

    if start_raw and not start_date:
        errors.append("grant_start_date invalid (expected DDMMYYYY).")

    if end_raw and not end_date:
        errors.append("grant_end_date invalid (expected DDMMYYYY).")

    try:
        if start_dt and end_dt and start_dt > end_dt:
            errors.append("grant_start_date cannot be after grant_end_date.")
    except Exception:
        pass

    updated_row = dict(row)
    updated_row["is_valid"] = len(errors) == 0
    updated_row["validation_errors"] = "; ".join(errors)


    if grant_status in ("0", "1"):
        updated_row["grant_status"] = int(grant_status)

    return updated_row


def parse_project_response(response):
    """
    Extracts project details from BulkGetProjectDetails3.
    Returns: {uri, name, code} or {}.
    """
    if not response:
        return {}

    try:
        details = response[0].get("projectDetails")
        return {
            "uri":  details.get("uri"),
            "name": details.get("name"),
            "code": details.get("code")
        }
    except Exception:
        return {}

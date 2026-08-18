import rail
from odessa.project_team_update_v3 import config


def is_meaningful(value):
    if value is None:
        return False
    return str(value).strip().lower() not in ("", "none")


def billing_rate_name_candidates(customer_role, location):
    if not customer_role:
        return []
    role = customer_role.strip()
    known_suffixes = tuple(
        f"({s})".lower() for s in config.billing_rate_suffix_by_location.values())
    if role.lower().endswith(known_suffixes):
        return [role]
    suffix = config.billing_rate_suffix_by_location.get((location or "").upper())
    candidates = []
    if suffix:
        candidates.append(f"{role} ({suffix})")
    candidates.append(role)
    return candidates


def resolve_billing_rate_name(customer_role, location):
    candidates = billing_rate_name_candidates(customer_role, location)
    return candidates[0] if candidates else None


def resolve_billing_rate(row, billing_rates):
    default = config.default_billing_rate_uri
    if (row.get("role") or "").strip().lower() != "yes":
        return default, True
    for name in billing_rate_name_candidates(row.get("customerrole"), row.get("location")):
        uri = rail.find_first_by_attr_and_get_attr(billing_rates, "name", name, "uri", None)
        if uri:
            return uri, True
    return default, False


def raise_if_report_error():
    result = rail.result("generate_userdata_report")
    error = result.get("error") if isinstance(result, dict) else getattr(result, "error", None)
    if error:
        raise RuntimeError(f"Userdata report generation failed: {error}")


def resolve_project_uri(dag_run):
    name = (dag_run.conf.get("projectname") or "").strip().lower()
    result = rail.result("search_project") or {}
    for row in result.get("rows", []):
        cells = row.get("cells") or []
        if cells and (cells[0].get("textValue") or "").strip().lower() == name:
            return cells[0].get("uri") or ""
    return ""


def open_task_uris():
    tasks = rail.result("get_children_tasks") or []
    return [t["uri"] for t in tasks if not t.get("isClosed")]


def build_row_result(dag_run):
    status = rail.result("get_row_status")
    if isinstance(status, dict):
        status = status.get("value")
    return {
        "loginname": dag_run.conf.get("loginname"),
        "projectname": dag_run.conf.get("projectname"),
        "action": dag_run.conf.get("action"),
        "status": status,
    }


def build_row_payloads():
    rows = rail.load_all_records(rail.result("join_rows_with_users"))
    billing_rates = rail.result("get_company_billing_rates")
    payloads = []
    for row in rows:
        payload = {
            key: ("" if value is None else (value.strip() if isinstance(value, str) else value))
            for key, value in row.items()
        }
        billing_rate_uri, matched = resolve_billing_rate(row, billing_rates)
        payload["billingratename"] = resolve_billing_rate_name(
            row.get("customerrole"), row.get("location")) or ""
        payload["billingrateuri"] = billing_rate_uri
        payload["billingratefound"] = "yes" if matched else "no"
        payloads.append(payload)
    return payloads

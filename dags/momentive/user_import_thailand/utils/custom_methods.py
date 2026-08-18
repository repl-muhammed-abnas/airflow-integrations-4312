# pylint: disable=line-too-long
from pendulum import now
import rail
from momentive.user_import_thailand.config import time_zone

def filter_supervisor_rows(response, dag_run):
    return [r for r in response['rows'] if r['cells'][0]['textValue'] == dag_run.conf['Manager_ID']]


def supervision_policy_user(response):
    return rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'user', '')


def find_employee_type_uri(response):
    return rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result('log_employee_type_name'), 'uri', '')


def find_approvalpath_uri(response):
    return rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result('log_approvalpath_name'), 'uri', '')


def find_holiday_uri(response):
    return rail.find_first_by_attr_and_get_attr(response, 'name', rail.result('log_holiday_name'), 'uri', '')


def find_timesheet_template_uri(response):
    return rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result('log_timesheet_template_name'), 'uri', '')


def find_payrule_uri(response):
    return rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result('log_payrule_name'), 'uri', '')


def find_office_schedule_uri(response):
    return rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result('log_schedule_name'), 'uri', '')


def _enabled_list_uri(response, name):
    """Resolve a *ListService GetData response (group rows) to the URI whose name matches."""
    rows = [{"uri": row["cells"][0]["uri"], "name": row["cells"][0]["textValue"]} for row in (response.get('rows') or [])]
    return rail.find_first_by_attr_and_get_attr(rows, 'name', name, 'uri', '')


def find_service_center_uri(response, dag_run):
    return _enabled_list_uri(response, dag_run.conf['paygroup'])


def find_cost_center_uri(response, dag_run):
    return _enabled_list_uri(response, dag_run.conf['cost_center'])


def find_division_uri(response, dag_run):
    return _enabled_list_uri(response, dag_run.conf['legal_entity'])


def find_department_group_uri(response, dag_run):
    return _enabled_list_uri(response, dag_run.conf['Location'])


def supervisor_employee_id(response):
    """employeeId of the current supervisor from a BulkGetUsers3 response."""
    return (((response or [{}])[0].get('userDetails') or {}).get('employeeId')) or ''


def find_enabled_cost_center_uri(response, dag_run):
    """URI of the requested cost center from GetEnabledCostCenters (existence check)."""
    return rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf['cost_center'], 'uri', '')


# --------------------------------------------------------------------------- #
# WriteLogOperator properties / severity builders
# --------------------------------------------------------------------------- #

def supervisor_defer_props(dag_run):
    """Properties for the deferred supervisor-assignment log entry the master fans out."""
    return {
        "parentjobid": dag_run.conf['parentjobid'],
        "loginid": dag_run.conf['User_ID'],
        "supervisorempid": dag_run.conf['Manager_ID'],
        "useruri": dag_run.conf['useruri'],
        "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
        "type": "update",
        "sup_email": dag_run.conf['CF_LRV_Manager_Email'] or '',
        "sup_firstname": dag_run.conf['CF_LRV_Manager_First_Name'] or '',
        "sup_lastname": dag_run.conf['CF_LRV_Manager_Last_Name'] or '',
        "sup_change_effective_date": dag_run.conf['Effective_Date_of_Manager_Change']
        if dag_run.conf.get('Effective_Date_of_Manager_Change') else str(now(tz=time_zone).date()),
    }


def status_and_details(dag_run):
    """Aggregate the per-field log_entries / exception_log into one user_import_logs entry."""
    exceptions = [r.get('value', '') for r in (rail.load_all_records(rail.result('exception_log')) or [])]
    entries = [r.get('value', '') for r in (rail.load_all_records(rail.result('log_entries')) or [])]
    exceptions = [e for e in exceptions if e]
    entries = [e for e in entries if e]
    details = "; ".join(exceptions + entries) if (exceptions or entries) else "No field changes were received"
    return {
        "jobid": dag_run.conf['parentjobid'],
        "userid": dag_run.conf['User_ID'],
        "username": f"{dag_run.conf['First_Name']} {dag_run.conf['Last_Name']}",
        "action": "Update",
        "status": "Exception" if exceptions else "Success",
        "details": details,
        "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
    }


def import_severity():
    return "Exception" if len(rail.load_all_records(rail.result("exception_log")) or []) > 0 else "Success"

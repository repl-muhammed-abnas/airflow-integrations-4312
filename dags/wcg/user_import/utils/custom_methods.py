import rail

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf['log']

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))

    return final_log_records


def get_invalid_user_log_details(item):
    missing_fields = []

    if not item.get("email"):
        missing_fields.append("Email is missing")
    if not item.get("employeeid"):
        missing_fields.append("NetSuite Internal ID (employeeid) is missing")
    if not item.get("firstname"):
        missing_fields.append("First Name is missing")
    if not item.get("lastname"):
        missing_fields.append("Last Name is missing")
    if not item.get("department") and not item.get("department_id"):
        missing_fields.append("Department or Department ID is missing")
    if not item.get("subsidiary"):
        missing_fields.append("Subsidiary is missing")
    if not item.get("employee_type"):
        missing_fields.append("Employee Type is missing")

    return " | ".join(missing_fields) if missing_fields else "Invalid user data"


def get_invalid_user_log_properties(item):
    return {
        "employeeid": item.get("employeeid", ""),
        "firstname": item.get("firstname", ""),
        "lastname": item.get("lastname", ""),
        "action": "Validation",
        "status": "Exception",
        "details": get_invalid_user_log_details(item)
    }

def get_supervisor_status(item, dag_run):
    from wcg.user_import.utils.request_payload import get_task_state

    original_status = item['properties'].get('status', '')

    if original_status == 'Error':
        return 'Error'

    if get_task_state('log_supervisor_still_not_found') == 'success':
        return 'Exception'

    if get_task_state('log_supervisor_already_assigned') == 'success':
        return 'Success'

    return 'Success'


def get_supervisor_message(item, dag_run):
    from wcg.user_import.utils.request_payload import get_task_state

    action = item['properties'].get('action', 'Update')
    original_status = item['properties'].get('status', '')

    if original_status == 'Error':
        return item['properties'].get('details', '')

    if get_task_state('log_supervisor_still_not_found') == 'success':
        return f"User {'added' if action == 'Add' else 'updated'} partially, Supervisor not present in Replicon"

    if get_task_state('log_supervisor_already_assigned') == 'success':
        return f"User {'added' if action == 'Add' else 'updated'} successfully"

    return f"User {'added' if action == 'Add' else 'updated'} successfully"


def get_current_supervisor_from_schedule(user_details):
    if not user_details or not user_details.get("supervisorAssignmentSchedule"):
        return None

    schedule = user_details.get("supervisorAssignmentSchedule", [])
    if not schedule:
        return None

    current_date = {"year": 2026, "month": 1, "day": 1}
    current_supervisor = None
    latest_effective_date = None

    for entry in schedule:
        effective_date = entry.get("effectiveDate")
        end_date = entry.get("endDate")

        if end_date:
            if (end_date.get("year") < current_date["year"] or
                (end_date.get("year") == current_date["year"] and end_date.get("month") < current_date["month"]) or
                (end_date.get("year") == current_date["year"] and end_date.get("month") == current_date["month"] and end_date.get("day") < current_date["day"])):
                continue

        if not effective_date:
            current_supervisor = entry.get("supervisor", {}).get("user", {}).get("uri")
            continue

        if not latest_effective_date or (
            effective_date.get("year") > latest_effective_date.get("year") or
            (effective_date.get("year") == latest_effective_date.get("year") and
             effective_date.get("month") > latest_effective_date.get("month")) or
            (effective_date.get("year") == latest_effective_date.get("year") and
             effective_date.get("month") == latest_effective_date.get("month") and
             effective_date.get("day") > latest_effective_date.get("day"))
        ):
            latest_effective_date = effective_date
            current_supervisor = entry.get("supervisor", {}).get("user", {}).get("uri")

    return current_supervisor

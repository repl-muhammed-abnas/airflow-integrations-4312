import json
import rail
from galaxyusopcoinc.time_data_import_v1.validations import payload_validator


def _time_to_seconds(time_val):
    if isinstance(time_val, dict):
        return time_val.get('hour', 0) * 3600 + time_val.get('minute', 0) * 60 + time_val.get('second', 0)
    if isinstance(time_val, str):
        parts = time_val.split(':')
        h = int(parts[0]) if len(parts) > 0 else 0
        m = int(parts[1]) if len(parts) > 1 else 0
        s = int(parts[2]) if len(parts) > 2 else 0
        return h * 3600 + m * 60 + s
    return 0


def validate_required_fields_and_date(dag_run, config):
    payload = dag_run.conf.get('time_entry_data', {})

    is_valid, missing_fields, error_msg = payload_validator.validate_required_fields(payload)
    if not is_valid:
        rail.set_result(key="error_msg", val=error_msg)
        rail.set_result(key="missing_fields", val=', '.join(missing_fields) if missing_fields else '')
        return False

    entry_date = payload.get('entrydate') or payload.get('entry_date')
    is_valid, error_msg = payload_validator.validate_entry_date(
        entry_date,
        min_days_past=config.ENTRY_DATE_MIN_DAYS_PAST,
        max_days_future=config.ENTRY_DATE_MAX_DAYS_FUTURE
    )
    if not is_valid:
        rail.set_result(key="error_msg", val=error_msg)
        rail.set_result(key="missing_fields", val='')
        return False

    rail.set_result(key="error_msg", val='')
    rail.set_result(key="missing_fields", val='')

    return True


def validate_oef_fields_by_template(dag_run, config):
    payload = dag_run.conf.get('time_entry_data', {})

    user_record = rail.result("get_user_report")["user_record"]
    timesheet_template = user_record.get('timesheetTemplate', {})
    template_name = timesheet_template.get('displayText') or timesheet_template.get('name', '')

    mandatory_oefs = list(config.MANDATORY_OEFS_ALL)
    for prefix, oefs in config.MANDATORY_OEFS_BY_TEMPLATE_PREFIX.items():
        if template_name.lower().startswith(prefix.lower()):
            mandatory_oefs += oefs
            break

    is_valid, missing_oefs, error_msg = payload_validator.validate_required_oef_fields(payload, mandatory_oefs)
    if not is_valid:
        rail.set_result(key="error_msg", val=error_msg)
        rail.set_result(key="missing_fields", val=', '.join(missing_oefs))
        return False

    rail.set_result(key="error_msg", val='')
    rail.set_result(key="missing_fields", val='')
    return True


def validate_no_inout_overlap(dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    in_time = payload_validator.get_field_value(payload, 'in', 'intime', 'in_time')
    out_time = payload_validator.get_field_value(payload, 'out', 'outtime', 'out_time')

    if in_time is None or out_time is None:
        rail.set_result(key="error_msg", val='')
        return True

    new_in = _time_to_seconds(in_time)
    new_out = _time_to_seconds(out_time)

    existing_entries = rail.result("get_existing_time_entries") or []
    for entry in existing_entries:
        ex_in = _time_to_seconds(entry.get('in_time'))
        ex_out = _time_to_seconds(entry.get('out_time'))
        if new_in < ex_out and new_out > ex_in:
            entry_date = payload_validator.get_field_value(payload, 'entrydate', 'entry_date')
            error_msg = f"Time entry skipped. The time entry overlaps with an existing time entry for {entry_date}."
            rail.set_result(key="error_msg", val=error_msg)
            return False

    rail.set_result(key="error_msg", val='')
    return True


def build_customer_log_for_result(dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    ecid = dag_run.conf.get('master_ecid', '')

    status = "Success"
    details = ""

    try:
        validate_payload = rail.result("validate_initial_payload", "error_msg")
        if validate_payload:
            status = "Exception"
            details = validate_payload
    except Exception:
        pass

    if not details:
        try:
            user_result = rail.result("get_user_report")
            if user_result and user_result.get('user_uri') is None:
                user_id = payload.get('userid')
                status = "Exception"
                details = f"Time entry skipped. Employee '{user_id}' is not found or inactive in Replicon"
        except Exception:
            pass

    if not details:
        try:
            oef_template_error = rail.result("validate_oef_by_template", "error_msg")
            if oef_template_error:
                status = "Exception"
                details = oef_template_error
        except Exception:
            pass

    if not details:
        try:
            timesheet_result = rail.result("get_or_create_timesheet")
            if timesheet_result and timesheet_result.get('timesheet_uri') is None:
                timesheet_error = rail.result("get_or_create_timesheet", "timesheet_error_msg")
                status = "Exception"
                details = timesheet_error or "Time entry skipped. The timesheet for the specified period could not be validated."
        except Exception:
            pass

    if not details:
        try:
            project_result = rail.result("get_project_details")
            if project_result and not project_result.get('all_valid'):
                error_msg = rail.result("get_project_details", "error_msg")
                status = "Exception"
                details = error_msg or "Time entry skipped. Project, team, or task validation failed."
        except Exception:
            pass

    if not details:
        try:
            client_result = rail.result("get_client_details")
            if client_result and not client_result.get('all_valid'):
                error_msg = rail.result("get_client_details", "error_msg")
                status = "Exception"
                details = error_msg or "Time entry skipped. Project, team, or task validation failed."
        except Exception:
            pass

    if not details:
        try:
            task_result = rail.result("get_task_list")
            if task_result and not task_result.get('all_valid'):
                error_msg = rail.result("get_task_list", "error_msg")
                status = "Exception"
                details = error_msg or "Time entry skipped. Project, team, or task validation failed."
        except Exception:
            pass

    if not details:
        try:
            team_result = rail.result("get_task_team")
            if team_result and not team_result.get('is_team_member'):
                error_msg = rail.result("get_task_team", "error_msg")
                status = "Exception"
                details = error_msg or "Time entry skipped. Project, team, or task validation failed."
        except Exception:
            pass

    if not details:
        try:
            oef_types = rail.result("get_oef_definition_types", "skipped_oefs")
            if oef_types and len(oef_types) > 0:
                skipped_oef_names = [oef.get('oef_name') for oef in oef_types]
                oef_list = ", ".join(skipped_oef_names)
                status = "Exception"
                details = f"Time entry skipped. The following custom fields are not configured in the system: {oef_list}."
        except Exception:
            pass

    if not details:
        try:
            msg = []
            dropdown_result = rail.result("get_dropdown_oef_values")
            if dropdown_result:
                for item in dropdown_result:
                    if not item.get('dropdown_found'):
                        oef_name = item.get('oef_name')
                        oef_value = item.get('oef_value', 'Unknown')
                        status = "Exception"
                        msg.append(f"'{oef_value}' for '{oef_name}'")
            if msg:
                details = f"Time entry skipped. The following custom field values are not recognized in the system: {', '.join(msg)}."
        except Exception:
            pass

    if not details:
        try:
            global_result = rail.result("check_assignee_in_global")
            if global_result and global_result.get('assignee_tag_uri') is None:
                assignee_id = payload.get('assigneeid') or payload.get('assignee_id')
                status = "Exception"
                details = f"Time entry skipped. Assignee ID '{assignee_id}' is not found in Replicon."
        except Exception:
            pass

    if not details:
        try:
            overlap_error = rail.result("validate_no_inout_overlap", "error_msg")
            if overlap_error:
                status = "Exception"
                details = overlap_error
        except Exception:
            pass

    if not details:
        details = "Processed Successfully"

    return json.dumps({
        "payloadid": payload.get('payloadid') or payload.get('payload_id'),
        "userid": payload.get('userid') or payload.get('user_id'),
        "clientcode": payload.get('clientcode') or payload.get('client_code'),
        "projectcode": payload.get('projectcode') or payload.get('project_code'),
        "taskcode": payload.get('taskcode') or payload.get('task_code') or '',
        "subtask": payload.get('subtask') or payload.get('subtaskcode') or payload.get('subtask_code') or '',
        "entrydate": payload.get('entrydate') or payload.get('entry_date'),
        "status": status,
        "details": details,
        "ecid": ecid or ''
    })


def get_error_details(dag_run):
    payload = dag_run.conf.get('time_entry_data', {})

    return json.dumps({
        "payloadid": payload.get('payloadid') or payload.get('payload_id'),
        "userid": payload.get('userid') or payload.get('user_id'),
        "clientcode": payload.get('clientcode') or payload.get('client_code'),
        "projectcode": payload.get('projectcode') or payload.get('project_code'),
        "taskcode": payload.get('taskcode') or payload.get('task_code') or '',
        "subtask": payload.get('subtask') or payload.get('subtaskcode') or payload.get('subtask_code') or '',
        "entrydate": payload.get('entrydate') or payload.get('entry_date'),
        "status": "Error",
        "details": rail.render_template("{{ get_error_message() }}"),
        "ecid": dag_run.conf.get('master_ecid', '')
    })



import itertools
import rail
from galaxyusopcoinc.time_data_import_v1.utils.request_payload import get_field_value
from galaxyusopcoinc.time_data_import_v1.validations import payload_validator


def validate_and_extract_user_details(user_response, dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    user_id = get_field_value(payload, 'userid', 'user_id')
    entry_date = get_field_value(payload, 'entrydate', 'entry_date')

    is_valid, user_record, error_msg = payload_validator.validate_user_from_report(
        user_id, user_response or [], entry_date
    )

    if not is_valid:
        return {
            "user_uri": None,
            "user_record": None
        }

    return {
        "user_uri": user_record.get('UserUri'),
        "user_record": user_record
    }


def validate_and_extract_timesheet_details(response, dag_run, config=None):
    payload = dag_run.conf.get('time_entry_data', {})
    timesheet = (response or {}).get('timesheet') or {}
    timesheet_uri = timesheet.get('uri')

    if not timesheet_uri:
        rail.set_result(key="timesheet_error_msg", val="No timesheet could be found or created for the specified entry date.")
        return {"timesheet_uri": None, "template_type": None}

    user_record = rail.result("get_user_report")["user_record"] or {}
    timesheet_template = user_record.get('timesheetTemplate', {})
    template_name = timesheet_template.get('displayText') or timesheet_template.get('name', '')

    template_config = getattr(config, 'TIMESHEET_TEMPLATES', None) if config else None
    is_valid, template_type, error_msg = payload_validator.validate_template_and_payload_fields(
        template_name, payload, template_config
    )

    if not is_valid:
        rail.set_result(key="timesheet_error_msg", val=error_msg)
        return {"timesheet_uri": None, "template_type": None}

    rail.set_result(key="timesheet_error_msg", val="")
    return {"timesheet_uri": timesheet_uri, "template_type": template_type}


def validate_and_extract_project_details(response, dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    project_code = get_field_value(payload, 'projectcode', 'project_code')
    client_code = get_field_value(payload, 'clientcode', 'client_code')

    rows = (response or {}).get('rows') or []
    matched = [r for r in rows if r['cells'][1].get('textValue') == project_code]

    if not matched:
        rail.set_result(key="error_msg", val=f"Project '{project_code}' was not found in the system.")
        return {"project_uri": None, "client_uri": None, "all_valid": False, "needs_client_validation": False}

    row = matched[0]
    project_uri = row['cells'][0].get('uri')
    status_text = (row['cells'][2].get('textValue') or '').lower()

    if status_text != 'in progress':
        raw_status = row['cells'][2].get('textValue', '')
        rail.set_result(key="error_msg", val=f"Project '{project_code}' is not available for time entry (Status: {raw_status}).")
        return {"project_uri": project_uri, "client_uri": None, "all_valid": False, "needs_client_validation": False}

    client_cell = row['cells'][3]
    client_uri = client_cell.get('uri')

    if client_code and not client_uri:
        rail.set_result(key="error_msg", val=f"Time entry skipped. Client '{client_code}' is not associated with project '{project_code}' in Replicon.")
        return {"project_uri": project_uri, "client_uri": None, "all_valid": False, "needs_client_validation": False}

    needs_client_validation = bool(client_code and client_uri)

    rail.set_result(key="error_msg", val="")
    return {"project_uri": project_uri, "client_uri": client_uri, "all_valid": True, "needs_client_validation": needs_client_validation}


def validate_client_details(response, dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    project_code = get_field_value(payload, 'projectcode', 'project_code')
    client_code = get_field_value(payload, 'clientcode', 'client_code')

    replicon_client_code = (response or {}).get('code') or ''

    if client_code != replicon_client_code:
        rail.set_result(key="error_msg", val=f"Time entry skipped. Client '{client_code}' is not associated with project '{project_code}' in Replicon.")
        return {"all_valid": False}

    rail.set_result(key="error_msg", val="")
    return {"all_valid": True}


def validate_and_extract_task_details(response, dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    project_code = get_field_value(payload, 'projectcode', 'project_code')
    task_code = get_field_value(payload, 'taskcode', 'task_code')
    subtask_code = get_field_value(payload, 'subtask', 'subtaskcode', 'subtask_code')

    if not task_code:
        rail.set_result(key="error_msg", val=f"Time entry skipped. Task code is missing for project '{project_code}'.")
        return {"task_uri": None, "all_valid": False}

    rows = (response or {}).get('rows') or []

    parent_row = next((
        r for r in rows
        if r['cells'][1].get('textValue') == task_code
        and r['cells'][2].get('dataType') == 'urn:replicon:list-type:null'
    ), None)

    if not parent_row:
        display = f"{task_code}/{subtask_code}" if subtask_code else task_code
        rail.set_result(key="error_msg", val=f"Time entry skipped. Task '{display}' is not found in project '{project_code}'.")
        return {"task_uri": None, "all_valid": False}

    if not parent_row['cells'][3].get('boolValue', True):
        rail.set_result(key="error_msg", val=f"Time entry skipped. Task '{task_code}' is closed and does not accept new time entries.")
        return {"task_uri": None, "all_valid": False}

    parent_task_uri = parent_row['cells'][0].get('uri')

    if subtask_code:
        subtask_row = next((
            r for r in rows
            if r['cells'][1].get('textValue') == subtask_code
            and r['cells'][2].get('uri') == parent_task_uri
        ), None)

        if not subtask_row:
            rail.set_result(key="error_msg", val=f"Time entry skipped. Subtask '{subtask_code}' is not under task '{task_code}' in project '{project_code}'.")
            return {"task_uri": None, "all_valid": False}

        if not subtask_row['cells'][3].get('boolValue', True):
            rail.set_result(key="error_msg", val=f"Time entry skipped. Subtask '{subtask_code}' is closed and does not accept new time entries.")
            return {"task_uri": None, "all_valid": False}

        task_uri = subtask_row['cells'][0].get('uri')
    else:
        task_uri = parent_task_uri

    rail.set_result(key="error_msg", val="")
    return {"task_uri": task_uri, "all_valid": True}


def validate_and_extract_task_details_paged(results, dag_run):
    combined_rows = list(itertools.chain(*((r or {}).get('rows') or [] for r in (results or []))))
    return validate_and_extract_task_details({"rows": combined_rows}, dag_run)


def validate_user_in_task_team(response, dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    user_id = get_field_value(payload, 'userid', 'user_id')
    project_code = get_field_value(payload, 'projectcode', 'project_code')

    user_report = rail.result("get_user_report")
    user_uri = user_report["user_uri"]
    user_record = user_report["user_record"] or {}
    user_cc_uris = set(user_record.get('costCenterUris', []))
    user_sc_uris = set(user_record.get('serviceCenterUris', []))

    for task_entry in (response or []):
        for assignment in (task_entry.get('assignments') or []):
            resource = assignment.get('resource') or {}
            u = resource.get('user')
            if u and u.get('uri') == user_uri:
                return {"is_team_member": True}
            cc = resource.get('costCenter')
            if cc and cc.get('uri') in user_cc_uris:
                return {"is_team_member": True}
            sc = resource.get('serviceCenter')
            if sc and sc.get('uri') in user_sc_uris:
                return {"is_team_member": True}

    rail.set_result(key="error_msg", val=f"Time entry skipped. User '{user_id}' is not a member of project '{project_code}' team.")
    return {"is_team_member": False}


def check_assignee_exists_in_project(response, dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    assignee_id = get_field_value(payload, 'assigneeid')
    assignee_name = get_field_value(payload, 'assigneeName')

    expected_display_text = f"{assignee_id} - {assignee_name}"

    if not response or len(response) == 0:
        rail.set_result(key="assignee_tag_uri", val=None)
        return {
            "assignee_tag_uri": None,
            "found_in_project": False
        }

    for tag_item in response:
        tag = tag_item.get('tag', {})
        display_text = tag.get('displayText', '')

        if display_text == expected_display_text:
            tag_uri = tag.get('uri')
            rail.set_result(key="assignee_tag_uri", val=tag_uri)
            return {
                "assignee_tag_uri": tag_uri,
                "found_in_project": True,
                "tag_slug": tag.get('slug'),
                "tag_display_text": display_text
            }

    rail.set_result(key="assignee_tag_uri", val=None)
    return {
        "assignee_tag_uri": None,
        "found_in_project": False
    }


def extract_assignee_tag_from_global(response, dag_run):

    payload = dag_run.conf.get('time_entry_data', {})
    assignee_id = get_field_value(payload, 'assigneeid', 'assignee_id')

    if not response or not response.get('rows'):
        rail.set_result(key="assignee_tag_uri", val=None)
        return {
            "assignee_tag_uri": None,
            "found_in_global": False
        }

    rows = response.get('rows', [])
    for row in rows:
        cells = row.get('cells', [])
        if len(cells) < 4:
            continue

        name = cells[0].get('textValue', '')
        tag_uri = cells[3].get('uri', '')

        if name == assignee_id and tag_uri:
            rail.set_result(key="assignee_tag_uri", val=tag_uri)
            return {
                "assignee_tag_uri": tag_uri,
                "found_in_global": True,
                "tag_name": name,
                "tag_code": cells[1].get('textValue', '')
            }

    rail.set_result(key="assignee_tag_uri", val=None)
    return {
        "assignee_tag_uri": None,
        "found_in_global": False
    }


def extract_oef_types_and_filter_by_payload(response, dag_run):
    payload_oefs = dag_run.conf['time_entry_data'].get('oef', [])

    replicon_oef_map = {}
    rows = response.get('rows', [])
    for row in rows:
        cells = row.get('cells', [])
        if len(cells) < 4:
            continue

        oef_name = cells[3].get('textValue', '')
        oef_uri = cells[0].get('uri', '')
        oef_type_uri = cells[1].get('uri', '')

        replicon_oef_map[oef_name] = {
            'uri': oef_uri,
            'type_uri': oef_type_uri
        }

    assignee_name_oef_uri = replicon_oef_map.get('Assignee Name', {}).get('uri')
    rail.set_result(key="assignee_name_oef_uri", val=assignee_name_oef_uri)

    if not payload_oefs or len(payload_oefs) == 0:
        rail.set_result(key="dropdown_oefs", val=[])
        rail.set_result(key="text_oefs", val=[])
        rail.set_result(key="skipped_oefs", val=[])
        return {
            "assignee_name_oef_uri": assignee_name_oef_uri,
            "dropdown_oefs": [],
            "text_oefs": [],
            "skipped_oefs": []
        }

    dropdown_oefs = []
    text_oefs = []
    skipped_oefs = []

    for payload_oef in payload_oefs:
        oef_name = payload_oef.get('name')
        oef_value = payload_oef.get('value')

        if oef_name not in replicon_oef_map:
            skipped_oefs.append({
                'oef_name': oef_name,
                'oef_value': oef_value
            })
            continue

        replicon_oef = replicon_oef_map[oef_name]
        oef_uri = replicon_oef['uri']
        oef_type_uri = replicon_oef['type_uri']

        if oef_type_uri == "urn:replicon:object-extension-definition-type:object-extension-type-tag":
            dropdown_oefs.append({
                'oef_name': oef_name,
                'oef_uri': oef_uri,
                'oef_value': oef_value
            })
        elif oef_type_uri == "urn:replicon:object-extension-definition-type:object-extension-type-numeric":
            text_oefs.append({
                'oef_name': oef_name,
                'oef_uri': oef_uri,
                'oef_value': oef_value,
                'oef_type': 'number'
            })
        else:
            text_oefs.append({
                'oef_name': oef_name,
                'oef_uri': oef_uri,
                'oef_value': oef_value,
                'oef_type': 'text'
            })

    rail.set_result(key="dropdown_oefs", val=dropdown_oefs)
    rail.set_result(key="text_oefs", val=text_oefs)
    rail.set_result(key="skipped_oefs", val=skipped_oefs)

    return {
        "assignee_name_oef_uri": assignee_name_oef_uri,
        "dropdown_oefs": dropdown_oefs,
        "text_oefs": text_oefs,
        "skipped_oefs": skipped_oefs,
        "dropdown_count": len(dropdown_oefs),
        "text_count": len(text_oefs)
    }


def extract_dropdown_values_and_validate(dropdown_response, dag_run, item):
    oef_name = item['oef_name']
    oef_value = item['oef_value']
    tags = dropdown_response.get('tags', [])

    if not tags or len(tags) == 0:
        return {
            'oef_name': oef_name,
            'oef_uri': item['oef_uri'],
            'oef_value': oef_value,
            'dropdown_uri': None,
            'dropdown_found': False
        }

    dropdown_uri = None
    for tag in tags:
        tag_name = tag.get('name', '')
        tag_display_text = tag.get('displayText', '')

        if tag_name == oef_value or tag_display_text == oef_value:
            dropdown_uri = tag.get('uri')
            break
    return {
        'oef_name': oef_name,
        'oef_uri': item['oef_uri'],
        'oef_value': oef_value,
        'dropdown_uri': dropdown_uri,
        'dropdown_found': bool(dropdown_uri)
    }


def extract_existing_time_entries(response):
    rows = (response or {}).get('rows', [])
    entries = []
    for row in rows:
        cells = row.get('cells', [])
        if len(cells) < 5:
            continue
        in_time = cells[3].get('timeValue')
        out_time = cells[4].get('timeValue')
        if in_time is not None and out_time is not None:
            entries.append({'in_time': in_time, 'out_time': out_time})
    return entries

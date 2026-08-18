import rail
from galaxyusopcoinc.time_data_import.utils.request_payload import get_field_value
from galaxyusopcoinc.time_data_import.validations import payload_validator, project_task_validator


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


def validate_and_extract_project_team_tasks(response, dag_run):
    payload = dag_run.conf.get('time_entry_data', {})
    project_code = get_field_value(payload, 'projectcode', 'project_code')
    task_code = get_field_value(payload, 'taskcode', 'task_code')
    subtask_code = get_field_value(payload, 'subtask', 'subtaskcode', 'subtask_code')
    client_code = get_field_value(payload, 'clientcode', 'client_code')
    user_id = get_field_value(payload, 'userid', 'user_id')

    results = (response or {}).get('results') or []
    project_entry = results[0] if results else None
    project = (project_entry or {}).get('project') or {}
    project_uri = project.get('uri')

    is_valid, _, error_msg = project_task_validator.validate_project(project_code, project)
    if not is_valid:
        rail.set_result(key="error_msg", val=error_msg)
        return _failed_project_result()

    project_client = project.get('client') or {}
    client_uri = project_client.get('uri')
    client_display_text = project_client.get('displayText') or ''
    client_prefix = f"{project_client.get('name') or ''} - "
    extracted_client_code = client_display_text[len(client_prefix):] if client_display_text.startswith(client_prefix) else ''
    if client_code and client_code != extracted_client_code:
        msg = f"Time entry skipped. Client '{client_code}' is not associated with project '{project_code}' in Replicon."
        rail.set_result(key="error_msg", val=msg)
        return _failed_project_result(project_uri=project_uri)

    user_report = rail.result("get_user_report")
    user_uri = user_report["user_uri"]
    user_record = user_report["user_record"] or {}
    user_cc_uris = set(user_record.get('costCenterUris', []))
    user_sc_uris = set(user_record.get('serviceCenterUris', []))

    tasks_tree = project_entry.get('tasks') or []

    if not _check_user_in_team(tasks_tree, user_uri, user_cc_uris, user_sc_uris):
        msg = f"Time entry skipped. User '{user_id}' is not a member of project '{project_code}' team."
        rail.set_result(key="error_msg", val=msg)
        return _failed_project_result(project_uri=project_uri, client_uri=client_uri)

    if not task_code:
        msg = f"Time entry skipped. Task code is missing for project '{project_code}'."
        rail.set_result(key="error_msg", val=msg)
        return _failed_project_result(project_uri=project_uri, client_uri=client_uri)

    parent_node = _find_task_at_top_level(tasks_tree, task_code)
    if not parent_node:
        task_display = f"{task_code}/{subtask_code}" if subtask_code else task_code
        msg = f"Time entry skipped. Task '{task_display}' is not found in project '{project_code}'."
        rail.set_result(key="error_msg", val=msg)
        return _failed_project_result(project_uri=project_uri, client_uri=client_uri)

    parent_task = parent_node.get('task') or {}
    if parent_task.get('isClosed'):
        msg = f"Time entry skipped. Task '{task_code}' is closed and does not accept new time entries."
        rail.set_result(key="error_msg", val=msg)
        return _failed_project_result(project_uri=project_uri, client_uri=client_uri)

    if subtask_code:
        child_tasks = parent_node.get('childTasks') or []
        subtask_node = _find_task_in_subtree(child_tasks, subtask_code) if child_tasks else None
        if not subtask_node:
            msg = f"Time entry skipped. Subtask '{subtask_code}' is not under task '{task_code}' in project '{project_code}'."
            rail.set_result(key="error_msg", val=msg)
            return _failed_project_result(project_uri=project_uri, client_uri=client_uri)

        subtask_info = subtask_node.get('task') or {}
        if subtask_info.get('isClosed'):
            msg = f"Time entry skipped. Subtask '{subtask_code}' is closed and does not accept new time entries."
            rail.set_result(key="error_msg", val=msg)
            return _failed_project_result(project_uri=project_uri, client_uri=client_uri)

        task_uri_to_use = subtask_info.get('uri')
    else:
        task_uri_to_use = parent_task.get('uri')

    rail.set_result(key="error_msg", val="")
    return {
        "project_uri": project_uri,
        "client_uri": client_uri,
        "task_uri": task_uri_to_use,
        "all_valid": True,
    }


def _failed_project_result(project_uri=None, client_uri=None):
    return {
        "project_uri": project_uri,
        "client_uri": client_uri,
        "task_uri": None,
        "all_valid": False,
    }


def _find_task_at_top_level(tasks_tree, code):
    for node in tasks_tree:
        task = node.get('task') or {}
        if task.get('code') == code:
            return node
    return None


def _find_task_in_subtree(children, code):
    for node in children:
        task = node.get('task') or {}
        if task.get('code') == code:
            return node
        nested = node.get('childTasks') or []
        if nested:
            hit = _find_task_in_subtree(nested, code)
            if hit:
                return hit
    return None


def _check_user_in_team(tasks_tree, user_uri, user_cc_uris, user_sc_uris):
    stack = list(tasks_tree)
    while stack:
        node = stack.pop()
        for ra in (node.get('resourceAssignments') or []):
            resource = ra.get('resource') or {}
            u = resource.get('user')
            if u and u.get('uri') == user_uri:
                return True
            cc = resource.get('costCenter')
            if cc and cc.get('uri') in user_cc_uris:
                return True
            sc = resource.get('serviceCenter')
            if sc and sc.get('uri') in user_sc_uris:
                return True
        stack.extend(node.get('childTasks') or [])
    return False


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

import uuid
import rail

FEED_DATE_FORMAT = '%m/%d/%Y'

mandatory_fields = {
    'project_name': 'project_name',
    'project_description': 'project_description',
    'project_code': 'project_code',
    "project_status": "project_status",
    'project_start_date': 'project_start_date',
    'project_end_date': 'project_end_date',
    'program': 'program',
    'task_name': 'task_name',
    'task_description': 'task_description',
    'task_code': 'task_code',
    'child_tasks': 'child_tasks',
    'task_allow_time_entry': 'task_allow_time_entry'
}


def get_project_data():
    return rail.load_all_records(rail.result("get_project_data_from_query"))[0]


def get_invalid_logs_property_conf(item):
    # Check if this is an invalid assignTeam error
    if item.get('assign_team') and item['assign_team'] not in ['', 'Assign them to all tasks', 'Do not assign them to all tasks']:
        details = f"Invalid assignTeam value received - '{item.get('assign_team', '')}'"
    else:
        # This is a mandatory field error
        def get_missing_field():
            not_present_fields = []
            for field in mandatory_fields:
                if item[field] in [None, '']:
                    not_present_fields.append(field)
            not_present_fields = list(filter(None, not_present_fields))
            return ";".join(not_present_fields)
        details = get_missing_field() + " not present in feed file"

    return {
        "projectcode": item['project_code'],
        "projectname": item['project_name'],
        "program": item['program'],
        "taskcode": item['task_code'],
        "taskname": item['task_name'],
        'action': 'Validation',
        "details": details,
        "Status": 'Exception'
    }


def does_wbs_exist():
    return bool(rail.result('get_project_details'))


def get_create_project_target_param():
    if does_wbs_exist():
        return {
            "uri": rail.result('get_project_details')['uri']
        }
    return None


def get_project_status():
    return {
        'Approved':	'In Progress',
        'At-Risk':	'In Progress',
        'Awarded':	'In Progress',
        'Canceled':	'Cancelled',
        'Rejected':	'Cancelled',
        'Closed':	'Completed',
        'Pending Close':	'Completed',
        'On Hold':	'Deferred',
        'Forecast':	'Tentative',
        'Unapproved':	'Tentative'
    }

def find_department_by_code(department_data, department_code):
    """
    Find department URI by code at level 2 or level 3.
    Returns department URI if found, None otherwise.
    """
    if not department_code:
        return None

    # Traverse the department hierarchy: Level 2 -> Level 3
    for level2 in department_data.get("childDepartments", []):
        l2_dept = level2.get("department", {})
        l2_code = l2_dept.get("code")

        # Check level 2
        if l2_code == department_code:
            return l2_dept.get("uri")

        # Check level 3
        for level3 in level2.get("childDepartments", []):
            l3_dept = level3.get("department", {})
            l3_code = l3_dept.get("code")

            if l3_code == department_code:
                return l3_dept.get("uri")

    return None

def create_projectorapply_modifications(dag_run, program_mapper):
    project_data = get_project_data()

    # Get owningOrg department URI if owningOrg is provided
    owning_org_uri = None
    if project_data.get('owning_org'):
        owning_org_uri = rail.result("validate_owning_org")['owning_org_uri'] if rail.result(
            "validate_owning_org")['owning_org_uri'] else None

    modifications = {
        "nameToApply": {
            "value": project_data["project_name"] + '-' + project_data['project_description']
        },
        "codeToApply":  {
            "value": dag_run.conf["project_code"]
        } if not does_wbs_exist() else None,
        "startDateToApply": {
            "date": rail.parse_date(project_data['project_start_date'], FEED_DATE_FORMAT)
        },
        "endDateToApply": {
            "date": rail.parse_date(project_data['project_end_date'], FEED_DATE_FORMAT)
        },
        "statusToApply": {
            "name": get_project_status().get(project_data['project_status'], 'In Progress')
        },
        "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:non-billable"
        },
        "programToApply": {
            "program": {
                "name": project_data["program"]
            }
        } if project_data["program"] in program_mapper and not does_wbs_exist() else None,
        "departmentGroupToApply": {
            "departmentGroup": {
                "uri": owning_org_uri
            }
        } if owning_org_uri else None,
        "isTimeEntryAllowed": "0",
    }

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_keyvalue_for_project():
    return {
        "projectUri": rail.result('create_or_update_project')['uri'],
        "keyValue": {
            "keyUri": "urn:replicon:project-key-value-key:project-team-member-assignment-type",
            "value": {
                "uri": "urn:replicon:project-team-member-assignment-type:manually-assign-task" if get_project_data().get('assign_team') == 'Do not assign them to all tasks' else "urn:replicon:project-team-member-assignment-type:automatically-assign-task"
                }
            }
        }


def get_task_target_payload(action: str, task: dict, project_uri: str) -> dict:
    if not task.get('child_tasks') and action == 'add':
        return None

    task_chain = task['child_tasks'].split('.')
    nested_parent = None

    if len(task_chain) > 2:
        nested_parent = {"name": task_chain[1], "project": {"uri": project_uri}}
        for parent_name in task_chain[2:-1]:
            nested_parent = {"name": parent_name, "parent": nested_parent}

    payload = {
        "name": task['task_code'] if action != "add" and task['task_level'] > 1 else None,
        "uri": task['uri'] if action != "add" and task['task_level'] == 1 else None,
        "parent": nested_parent
    }

    return payload

def get_task_payload(action, data):
    return list(map(lambda task: {
        "target": get_task_target_payload(action, task, rail.result('create_or_update_project')['uri']),
        "taskModificationToApply": {
                "name": task['task_code'],
                "codeToApply": {
                    "value": task['task_name'] + '-' + task['task_description']
                },
                "isClosed": 0 if task['task_status'] == 'Open' else 1 if task['task_status'] == 'Closed' else None,
                "timeAndExpenseEntryTypeToApply": {
                    "value": "urn:replicon:time-and-expense-entry-type:non-billable"
                },
                "timeEntryStartDateToApply": {
                    "date": rail.parse_date(task['task_start_date'], FEED_DATE_FORMAT) if task['task_start_date'] else rail.parse_date(
                        get_project_data()['project_start_date'], FEED_DATE_FORMAT)
                },
                "timeEntryEndDateToApply": {
                    "date": rail.parse_date(task['task_end_date'], FEED_DATE_FORMAT) if task['task_end_date'] else rail.parse_date(
                        get_project_data()['project_end_date'], FEED_DATE_FORMAT)
                },
                "isTimeEntryAllowed": "1" if task['task_allow_time_entry'] == 'Y' else "0" if task['task_allow_time_entry'] == 'N' else None,
                }
    }, data))


def get_update_task_payload():
    return {
        "project": {
            "uri": rail.result('create_or_update_project')['uri'],
        },
        "taskHierarchy": get_task_payload("update", rail.result("get_all_task_to_add_update")['tasks_to_update']),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_add_task_payload():
    return {
        "project": {
            "uri": rail.result('create_or_update_project')['uri'],
        },
        "taskHierarchy": get_task_payload("add", rail.result("get_all_task_to_add_update")['tasks_to_add']),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

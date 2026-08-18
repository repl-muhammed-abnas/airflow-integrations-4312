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
    def get_missing_field():
        not_present_fields = []
        for field in mandatory_fields:
            if item[field] in [None, '']:
                not_present_fields.append(field)
        not_present_fields = list(filter(None, not_present_fields))
        return ";".join(not_present_fields)
    return {
        "projectcode": item['project_code'],
        "projectname": item['project_name'],
        "program": item['program'],
        "taskcode": item['task_code'],
        "taskname": item['task_name'],
        'action': 'Validation',
        "details": get_missing_field() + " not present in feed file",
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


def create_projectorapply_modifications(dag_run, program_mapper):
    modifications = {
        "nameToApply": {
            "value": get_project_data()["project_name"] + '-' + get_project_data()['project_description']
        },
        "codeToApply":  {
            "value": dag_run.conf["project_code"]
        } if not does_wbs_exist() else None,
        "startDateToApply": {
            "date": rail.parse_date(get_project_data()['project_start_date'], FEED_DATE_FORMAT)
        },
        "endDateToApply": {
            "date": rail.parse_date(get_project_data()['project_end_date'], FEED_DATE_FORMAT)
        },
        "statusToApply": {
            "name": get_project_status().get(get_project_data()['project_status'], 'In Progress')
        },
        "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:non-billable"
        },
        "programToApply": {
            "program": {
                "name": get_project_data()["program"]
            }
        } if get_project_data()["program"] in program_mapper and not does_wbs_exist() else None,
        "isTimeEntryAllowed": "0",
    }

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
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

from collections import defaultdict
from copy import deepcopy
import itertools
import json
import logging
import rail
import uuid


REPLICON_PROJECT_STATUS = {
    'A': 'In Progress',
    'I': 'Deferred',
    'D': 'Completed'
}

def get_required_users(dag_run):
    users = list(set([
        dag_run.conf['ProjMgr'],
        dag_run.conf['Principal'],
        dag_run.conf['Supervisor']
    ]))
    return {
        'users': list(map(lambda x: { 'loginName': x }, users)),
        'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission'
    }


def getTimeEntryDateRange(startDateTime, endDateTime):
    startDate = startDateTime.split('T')[0] if startDateTime else None
    endDate = endDateTime.split('T')[0] if endDateTime else None
    return {
        'startDate': {
            'year': startDate.split('-')[0],
            'month': startDate.split('-')[1],
            'day': startDate.split('-')[2]
        } if startDate else None,
        'endDate': {
            'year': endDate.split('-')[0],
            'month': endDate.split('-')[1],
            'day': endDate.split('-')[2]
        } if endDate else None
    }


def get_task_modifications_param(dag_run):
    task_uri = None
    subtask_uri = None
    replicon_task_code = None
    replicon_subtask_code = None
    replicon_task_name = None
    replicon_subtask_name = None

    conf = dag_run.conf
    is_phase_leaf = True
    is_subtask = conf['WBS3'] != ' '
    is_phase = not is_subtask
    vp_task_code = conf['WBS2']
    vp_subtask_code = f"{conf['WBS2']}/{conf['WBS3']}"
    ready_for_processing = conf['ReadyForProcessing'] == 'Y'

    if rail.result('fetch_task_details'):
        code_map = {}
        for task in rail.result('fetch_task_details'):
            if task['hierarchyLevel'] == 0:
                code_map[task['name']] = task['code']
                if task['code'] == vp_task_code:
                    task_uri = task['uri']
                    is_phase_leaf = not task['hasChildren']
                if task['name'] == conf['Name']:
                    replicon_task_code = task['code']
                    replicon_task_name = task['name']
                elif task['name'] == f"{conf['Name']} ({conf['WBS2']})":
                    replicon_task_code = task['code']
                    replicon_task_name = f"{conf['Name']} ({conf['WBS2']})"

        if task_uri:
            for task in rail.result('fetch_task_details'):
                if task['hierarchyLevel'] == 1 and code_map[task['parent']] == vp_task_code:
                    if task['code'] == vp_subtask_code:
                        subtask_uri = task['uri']
                    if task['name'] == conf['Name']:
                        replicon_subtask_code = task['code']
                        replicon_subtask_name = task['name']
                    elif task['name'] == f"{conf['Name']} ({conf['WBS3']})":
                        replicon_subtask_code = task['code']
                        replicon_subtask_name = f"{conf['Name']} ({conf['WBS3']})"

    target = None
    existing_name = replicon_subtask_name if is_subtask else replicon_task_name
    if is_subtask:
        target = { 'uri': subtask_uri, 'parent': { 'uri': task_uri } }
        if replicon_subtask_code != vp_subtask_code and replicon_subtask_name == conf['Name']:
            existing_name = f"{conf['Name']} ({conf['WBS3']})"

    else:
        target = { 'uri': task_uri } if task_uri else None
        if replicon_task_code != vp_task_code and replicon_task_name == conf['Name']:
            existing_name = f"{conf['Name']} ({conf['WBS2']})"

    start_end_date = getTimeEntryDateRange(conf['StartDate'], conf['EndDate'])
    return {
        'target': target,
        'project': { 'code': conf['WBS1'] },
        'modifications': {
            'name': existing_name if existing_name else conf['Name'],
            'codeToApply': { 'value' : vp_subtask_code if is_subtask else vp_task_code  },
            'isClosed': conf['Status'] != 'A' or (is_phase and not ready_for_processing),
            'isTimeEntryAllowed': ready_for_processing if is_subtask else is_phase_leaf and ready_for_processing,
            'timeEntryStartDateToApply': start_end_date['startDate'],
            'timeEntryEndDateToApply': start_end_date['endDate'],
            'timeAndExpenseEntryTypeToApply': {
                'value': 'urn:replicon:time-and-expense-entry-type:billable-and-non-billable'
            } if conf['ChargeType'] == conf['CHARGE_TYPES']['REGULAR'] else None,
            'resourceAssignmentModifications': {
                'resourcesToAdd': [
                    { 'department': { 'uri': conf['all_users_uri'] } }
                ]
            }
        },
        'unitOfWorkId': str(uuid.uuid4())
    }


def get_tasks(parentId, project_tasks, all_users_uri):
    task_hierarchy = []
    parents = parentId.split('|')
    is_subtask = bool(len(parents) > 1)

    project_subtasks = list(filter(lambda x: x.get('parentId', '').startswith(parentId), project_tasks))
    tasks = list(filter(lambda x: x.get('parentId') == parentId, project_subtasks))

    for x in tasks:
        ready_for_processing = x['ReadyForProcessing'] == 'Y'
        child_tasks = get_tasks(f"{parentId}|{x['WBSNumber']}", project_subtasks, all_users_uri)

        task_hierarchy.append({
            'task': {
                'target': { 'name': x['Name'] },
                'name': x['Name'],
                'code': f"{parents[-1]}/{x['WBSNumber']}" if is_subtask else x['WBSNumber'],
                'percentCompleted': 0,
                'isTimeEntryAllowed': ready_for_processing if is_subtask else len(child_tasks) == 0 and ready_for_processing,
                'isClosed': x['Status'] != 'A' or (not is_subtask and x['ReadyForProcessing'] == 'N'),
                'timeEntryDateRange': getTimeEntryDateRange(x['StartDate'], x['EndDate']),
                'assignedResources': [{ 'department': { 'uri': all_users_uri } }]
            },
            'childTasks': child_tasks
        })
    return task_hierarchy


def get_billing_type(CHARGE_TYPES, charge_type):
    if charge_type == CHARGE_TYPES['REGULAR']:
        return {
            'billingTypeUri': 'urn:replicon:billing-type:time-and-material',
            'timeAndMaterials': {
                'timeAndExpenseEntryTypeUri': 'urn:replicon:time-and-expense-entry-type:billable-and-non-billable'
            }
        }
    return { 'billingTypeUri': 'urn:replicon:billing-type:non-billable' }


def get_sync_project_and_task_param(dag_run):
    def get_existing_task(code, task_level):
        if rail.result('fetch_task_details'):
            for x in rail.result('fetch_task_details'):
                if x['hierarchyLevel'] == task_level and x['code'] == code:
                    return x['uri']
        return None

    def update_tasks(tasks, task_level = 0):
        task_name_count = defaultdict(int)
        for x in tasks:
            task_name_count[x['task']['name']] += 1

        task_hierarchy = []
        for x in tasks:
            updated = deepcopy(x)
            vp_name = x['task']['name']
            code = x['task']['code']
            if task_name_count[vp_name] > 1:
                replicon_code = code.split('/')[1] if task_level == 1 else code
                updated_name = f"{vp_name} ({replicon_code})"
                updated['task']['name'] = updated_name
                existing_task_uri = get_existing_task(code, task_level)
                updated['task']['target']['uri'] = existing_task_uri if existing_task_uri else None
                updated['task']['target']['name'] = None if existing_task_uri else updated_name
            updated['childTasks'] = update_tasks(x['childTasks'], task_level + 1)
            task_hierarchy.append(updated)
        return task_hierarchy

    allow_time_entry = dag_run.conf['ReadyForProcessing'] == 'Y'
    return {
        'project': {
            'target': { 'code': dag_run.conf['WBSNumber'] },
            'projectInfo': {
                'name': dag_run.conf['Name'],
                'code': dag_run.conf['WBSNumber'],
                'timeEntryDateRange': getTimeEntryDateRange(dag_run.conf['StartDate'], dag_run.conf['EndDate']),
                'projectStatusLabel': {
                    'name': REPLICON_PROJECT_STATUS.get(dag_run.conf['Status'] if allow_time_entry else 'D')
                } if dag_run.conf['Status'] else None,
                'clients': [
                    {
                        'client': { 'code': dag_run.conf['ClientID'] },
                        'costAllocationPercentage': 100
                    }
                ] if dag_run.conf['ClientID'] else None,
                "projectLeader": {
                    "loginName": dag_run.conf['ProjMgr']
                } if rail.result('managers_and_comanagers')['MANAGER'] else None,
                'percentCompleted': 0,
                'isTimeEntryAllowed': allow_time_entry and len(dag_run.conf['task_hierarchy']) == 0,
                'isProjectLeaderApprovalRequired': True,
                **get_billing_type(dag_run.conf['CHARGE_TYPES'], dag_run.conf['ChargeType'])
            },
            'tasks': update_tasks(dag_run.conf['task_hierarchy']),
            'team': {
                'teamMembers': [{
                    'resource': { 'uri': dag_run.conf['all_users_uri'] }
                }]
            }
        }
    }


def get_phase_time_entry_param(dag_run):
    allow_time_entry = None
    phase = next(iter(filter(
        lambda x: x['code'] == dag_run.conf['WBS2'] and x['hierarchyLevel'] == 0,
        rail.result('fetch_task_details')
    )), {})

    if dag_run.conf['Action'] == dag_run.conf['WEBHOOK_ACTION']['DELETE']:
        allow_time_entry = True
    else:
        allow_time_entry = False
    return json.dumps({
        'taskUri': phase.get('uri'),
        'allowTimeEntry': allow_time_entry and dag_run.conf['phase_ready_for_processing']
    })

def get_project_time_entry_param(dag_run):
    disallow_time_entry = None

    if dag_run.conf['Action'] == dag_run.conf['WEBHOOK_ACTION']['DELETE']:
        disallow_time_entry = False
    else:
        disallow_time_entry = True

    return json.dumps({
        'projectUri': rail.result('fetch_project_details')[0]['projectDetails']['uri'],
        'allowTimeEntryAgainstTasksOnly': disallow_time_entry
    })

def get_modify_parent_time_entry_params(dag_run):
    url = None
    params = None
    is_subtask = dag_run.conf['WBS3'] != ' '
    if is_subtask:
        url = "/services/TaskService1.svc/UpdateAllowTimeEntry"
        params = get_phase_time_entry_param(dag_run)
    else:
        url = "/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly"
        params = get_project_time_entry_param(dag_run)

    return { "url": url, "params": params }


def get_update_oef_param(dag_run, role):
    user_id_field = 'Supervisor' if role == 'SUPERVISOR' else 'Principal'
    updated_user = dag_run.conf[user_id_field]
    is_user_enabled = list(filter(
        lambda x: x['isEnabled'] and x['loginName'] == updated_user,
        rail.result('fetch_replicon_users')
    )) if updated_user else True

    user = [x['uri'] for x in rail.result('fetch_project_oefs') \
        if x['name'] == dag_run.conf['ROLES'][role]]

    return {
        'objectUri': rail.result('sync_project_and_task')['uri'],
        'value': {
            'definition': { 'uri': user[0] },
            'textValue': updated_user if is_user_enabled else ''
        }
    }


def build_login_filter_payload(employees):
    def make_filter(emp):
        return {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {'text': emp}
            }
        }

    filter_expr = None
    for emp in employees:
        f = make_filter(emp)
        filter_expr = f if filter_expr is None else {
            'leftExpression': filter_expr,
            'operatorUri': 'urn:replicon:filter-operator:or',
            'rightExpression': f
        }

    return {
        'page': '1',
        'pagesize': '10000',
        'columnUris': ['urn:replicon:user-list-column:user'],
        'sort': [],
        'filterExpression': filter_expr
    }


def page_handler(request, response):
    if len(response['rows']) > 0:
        request['page'] += 1
        return request
    return None


def find_timesheet_lc_oef(oefs, oef_name):
    return next((oef for oef in (oefs or []) if oef.get('name') == oef_name), None)


def build_lc_tag_list_data(oef_uri):
    return {
        'page': 1,
        'pagesize': 10000,
        'columnUris': [
            'urn:replicon:object-extension-tag-list-column:name',
            'urn:replicon:object-extension-tag-list-column:code',
            'urn:replicon:object-extension-tag-list-column:description',
            'urn:replicon:object-extension-tag-list-column:object-extension-tag',
            'urn:replicon:object-extension-tag-list-column:enabled'
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:object-extension-tag-list-filter:definition'
            },
            'operatorUri': 'urn:replicon:filter-operator:equal',
            'rightExpression': {
                'value': { 'uri': oef_uri }
            }
        }
    }


def parse_lc_tag_details(response):
    if not response:
        return []
    rows = list(itertools.chain(*[page['rows'] for page in response]))
    return [{
        'name': row['cells'][0].get('textValue'),
        'code': row['cells'][1].get('textValue'),
        'description': row['cells'][2].get('textValue'),
        'uri': row['cells'][3].get('uri')
    } for row in rows]


def build_existing_lc_assignments_data(project_code, oef_name):
    return {
        'page': 1,
        'pageSize': 1000,
        'textSearch': None,
        'project': {
            'uri': None,
            'name': None,
            'code': project_code,
            'parameterCorrelationId': None
        },
        'objectExtensionFieldDefinition': {
            'uri': None,
            'name': oef_name
        }
    }


def _to_lc_tag_target(uri):
    return { 'uri': uri, 'slug': None, 'tagName': None }


def build_apply_lc_tag_modifications(tags_task_id, assignments_task_id, oef_name):
    def _build(dag_run):
        raw_labor_codes = rail.result('fetch_labor_codes')
        labor_codes = raw_labor_codes.get('data', []) if isinstance(raw_labor_codes, dict) else (raw_labor_codes or [])

        tags = rail.result(tags_task_id) or []
        name_to_uri = { tag['name']: tag['uri'] for tag in tags if tag.get('name') and tag.get('uri') }

        raw_assignments = rail.result(assignments_task_id)
        existing = raw_assignments.get('d', []) if isinstance(raw_assignments, dict) else (raw_assignments or [])

        tags_to_add = []
        desired_uris = set()
        missing = []
        for entry in labor_codes:
            key = f"{entry.get('LaborCode')}-{entry.get('LaborCodeName')}"
            tag_uri = name_to_uri.get(key)
            if not tag_uri:
                missing.append(key)
                continue
            if tag_uri in desired_uris:
                continue
            desired_uris.add(tag_uri)
            date_range = getTimeEntryDateRange(entry.get('StartDate'), entry.get('EndDate'))
            tags_to_add.append({
                'target': _to_lc_tag_target(tag_uri),
                'isEnabled': 'true',
                'dateRange': {
                    'startDate': date_range['startDate'],
                    'endDate': date_range['endDate'],
                    'relativeDateRangeUri': None,
                    'relativeDateRangeAsOfDate': None
                }
            })

        if missing:
            logging.getLogger(__name__).warning(
                "No matching '%s' OEF tag for labor code value(s) %s on project %s; not assigned.",
                oef_name, missing, dag_run.conf.get('WBS1')
            )

        tags_to_remove = [
            _to_lc_tag_target((assignment.get('tag') or {}).get('uri'))
            for assignment in existing
            if (assignment.get('tag') or {}).get('uri')
            and (assignment.get('tag') or {}).get('uri') not in desired_uris
        ]

        return {
            'project': {
                'uri': None,
                'name': None,
                'code': dag_run.conf['WBS1'],
                'parameterCorrelationId': None
            },
            'objectExtensionFieldTags': {
                'tagsToAdd': tags_to_add,
                'tagsToRemove': tags_to_remove
            }
        }
    return _build

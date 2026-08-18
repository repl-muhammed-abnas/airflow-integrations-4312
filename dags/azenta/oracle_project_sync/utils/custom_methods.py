from datetime import timedelta
from urllib.parse import parse_qsl, urlencode

import rail
from airflow.providers.http.hooks.http import HttpHook
from pendulum import now

from azenta.oracle_project_sync import config
from azenta.oracle_project_sync.mappers import field_mapper


# ---------------------------------------------------------------------------
# Oracle list-response helpers
# ---------------------------------------------------------------------------

def _items(response):
    """Return the 'items' list from an Oracle onlyData list response (tolerant)."""
    if not response:
        return []
    if isinstance(response, list):
        return response
    return response.get('items', []) or []


def fetch_oracle_paginated(http_conn_id, endpoint, headers=None):
    """Fetch every page of an Oracle Fusion REST list endpoint via a manual offset loop.

    SimpleHttpOperator's built-in `pagination_function` needs apache-airflow-providers-http
    >=5.x (apache-airflow>=2.9), but this repo's CI installs airflow 2.7.3 - so pagination is
    done here with HttpHook.run() directly, one page at a time, merging every page's items
    until a partial page (fewer than the request's `limit`) is seen.
    """
    hook = HttpHook(method='GET', http_conn_id=http_conn_id)
    items = []
    next_endpoint = endpoint
    while next_endpoint:
        response = hook.run(endpoint=next_endpoint, headers=headers or {'Accept': 'application/json'})
        page_items = _items(response.json())
        items.extend(page_items)
        path, _, query = next_endpoint.partition('?')
        params = dict(parse_qsl(query))
        limit = int(params.get('limit', config.ORACLE_PAGE_LIMIT))
        if len(page_items) < limit:
            break
        params['offset'] = str(int(params.get('offset', 0)) + limit)
        next_endpoint = f"{path}?{urlencode(params)}"
    return {'items': items}


def build_worklist(active_response, closed_response):
    """Merge the ACTIVE and CLOSED delta results into a deduped project worklist.

    Args:
        active_response: JSON from the ACTIVE projects delta query.
        closed_response: JSON from the CLOSED projects delta query.

    Returns:
        list[dict]: one entry per project with the minimal fields the child DAG needs.
    """
    worklist = {}
    for row in _items(active_response) + _items(closed_response):
        project_id = row.get('ProjectId')
        if project_id is None:
            continue
        worklist[project_id] = {
            'ProjectId': project_id,
            'ProjectNumber': row.get('ProjectNumber'),
            'ProjectStatusCode': row.get('ProjectStatusCode'),
            'ProjectName': row.get('ProjectName'),
            'LastUpdateDate': row.get('LastUpdateDate'),
        }
    return list(worklist.values())


# ---------------------------------------------------------------------------
# Status + classification gates
# ---------------------------------------------------------------------------

def should_skip_status(status_code):
    """True if the Oracle status must never be integrated (DRAFT/REJECTED)."""
    return (status_code or '').upper() in field_mapper.PROJECT_STATUSES_TO_SKIP


def map_project_status(status_code):
    """Map an Oracle ProjectStatusCode to a Replicon project status (default In Progress)."""
    return field_mapper.PROJECT_STATUS_MAP.get((status_code or '').upper(), 'In Progress')


def has_required_classification(classifications_response):
    """True if the project carries the required 'CUSP - POC' project classification."""
    for row in _items(classifications_response):
        if (
            row.get('ClassCategory') == field_mapper.REQUIRED_PROJECT_CLASSIFICATION_CATEGORY
            and row.get('ClassCode') == field_mapper.REQUIRED_PROJECT_CLASSIFICATION_CODE
        ):
            return True
    return False


# ---------------------------------------------------------------------------
# Project manager resolution (Oracle ProjectTeamMembers -> Polaris user)
# ---------------------------------------------------------------------------

def pick_active_project_manager_email(team_members_response):
    """Return the active Project Manager's email from Oracle ProjectTeamMembers.

    Keeps rows whose ProjectRole == 'Project Manager' (case-insensitive) and that are still
    active (FinishDate is blank OR >= today), then picks the one with the latest StartDate.
    Returns the PersonEmail or None. Never raises - a missing PM is handled downstream.
    """
    today = now('UTC').date().isoformat()
    candidates = []
    for row in _items(team_members_response):
        role = (row.get('ProjectRole') or '').strip().lower()
        if role != field_mapper.PROJECT_MANAGER_ROLE_NAME:
            continue
        finish = row.get('FinishDate')
        if finish and finish < today:
            continue
        if not row.get('PersonEmail'):
            continue
        candidates.append(row)

    if not candidates:
        return None
    candidates.sort(key=lambda r: (r.get('StartDate') or ''), reverse=True)
    return candidates[0].get('PersonEmail')



def pick_user_uri_from_user_list(list_response):
    """Extract the user URI from a UserListService1.svc/GetData response (or None).

    Relies on `user` being the first column in columnUris so cells[0]['uri'] is always
    the user URI — works for both global and tenant-scoped URI formats.
    """
    if not list_response:
        return None
    rows = list_response.get('rows')
    if not rows:
        return None
    cells = rows[0].get('cells') or []
    if not cells:
        return None
    return (cells[0] or {}).get('uri')


def pm_has_required_permission(permission_sets_response):
    """True if the user holds a permission set under the project-management policy.

    Covers both 'Project Manager' and 'Project Management Administrator' (same policy URN).
    """
    return bool(rail.find_first_by_attr_and_get_attr(
        permission_sets_response, 'policyUri',
        field_mapper.PM_PERMISSION_POLICY_URN, 'permissionSet'))


def estimate_is_updatable(estimates_response, role_uri):
    """True if no estimate exists for this role, or the existing one has no real user assigned.

    Spec rule: update placeholder hours only when a PM has NOT yet assigned a real resource.
    Once a user is assigned, Polaris owns those hours — the sync must not overwrite them.
    """
    estimates = (
        ((estimates_response or {}).get('data') or {}).get('task') or {}
    ).get('resourceEstimates') or []
    existing = next(
        (e for e in estimates if (e.get('projectRole') or {}).get('id') == role_uri), None
    )
    if not existing:
        return True
    return not (existing.get('resourceUser') or {}).get('id')


def pick_project_from_list_response(response):
	"""Extract project uri, code, name from ProjectListService1.svc/GetData response (or None).

	Returns dict {'uri', 'code', 'name'} from the first row, or None if no rows found.
	Relies on columnUris order: project (uri), code (textValue), name (textValue).
	"""
	if not response:
		return None
	rows = response.get('rows')
	if not rows:
		return None
	cells = rows[0].get('cells') or []
	if len(cells) < 3:
		return None
	return {
		'uri': (cells[0] or {}).get('uri'),
		'code': (cells[1] or {}).get('textValue'),
		'name': (cells[2] or {}).get('textValue'),
	}


def pick_task_from_list_response(response):
	"""Extract task uri, code, name from TaskListService1.svc/GetData response (or None).

	Returns dict {'uri', 'code', 'name'} from the first row, or None if no rows found.
	Relies on columnUris order: task (uri), code (textValue), name (textValue).
	"""
	if not response:
		return None
	rows = response.get('rows')
	if not rows:
		return None
	cells = rows[0].get('cells') or []
	if len(cells) < 3:
		return None
	return {
		'uri': (cells[0] or {}).get('uri'),
		'code': (cells[1] or {}).get('textValue'),
		'name': (cells[2] or {}).get('textValue'),
	}


def to_replicon_date_parts(date_str):
    """Convert an Oracle 'YYYY-MM-DD[T...]' date string to Replicon's {year, month, day}.

    Tolerates Oracle dates with time/timezone suffix (e.g. '2026-07-22T00:00:00+00:00').
    Returns None if date_str is falsy.
    """
    if not date_str:
        return None
    year, month, day = date_str[:10].split('-')
    return {'year': int(year), 'month': int(month), 'day': int(day)}


# ---------------------------------------------------------------------------
# Project name
# ---------------------------------------------------------------------------

def build_project_name(project):
    """Build the Replicon project name, optionally prefixed with the Oracle number."""
    name = project.get('ProjectName') or ''
    if field_mapper.REPLICON_PROJECT_NAME_CONCAT_NUMBER:
        return f"{project.get('ProjectNumber')} - {name}"
    return name


# ---------------------------------------------------------------------------
# Resource-group filters (operate on a normalised assignment dict)
# ---------------------------------------------------------------------------

def _name_lc(assignment):
    return (assignment.get('resource_name') or '').strip().lower()


def is_real_labor_assignment(assignment):
    """False for the generic 'Labor' bucket and 'Service Commissioning' resources."""
    name_lc = _name_lc(assignment)
    if not name_lc:
        return False
    if name_lc in field_mapper.GENERIC_LABOR_NAMES:
        return False
    for prefix in field_mapper.EXCLUDED_RESOURCE_NAME_PREFIXES:
        if name_lc.startswith(prefix):
            return False
    return True


def is_field_service_engineering(assignment):
    """True if the resource is one of the 8 Field Service Engineering RGs."""
    return _name_lc(assignment) in field_mapper.FIELD_SERVICE_ENGINEERING_RGS


def belongs_in_resource_groups_oef(assignment):
    """All real labor assignments (including FSE) go into the Resource Groups OEF text."""
    return is_real_labor_assignment(assignment)


def belongs_as_individual_placeholder(assignment):
    """Individual placeholders exclude FSE RGs (kept only in the OEF text)."""
    if not is_real_labor_assignment(assignment):
        return False
    return not is_field_service_engineering(assignment)


def resource_groups_oef_value(assignments):
    """Pipe-separated, sorted, de-duplicated resource-group names for the task OEF."""
    names = sorted({
        (a.get('resource_name') or '').strip()
        for a in assignments
        if belongs_in_resource_groups_oef(a)
    })
    return ' | '.join(n for n in names if n)



def _task_level(task):
    """TaskLevel as an int, defaulting missing/blank levels to 1 (project root)."""
    try:
        return int(task.get('TaskLevel'))
    except (TypeError, ValueError):
        return 1


def _clean_task_name(name):
    """Trim an Oracle task name (whitespace incl. the non-breaking space U+00A0).

    Oracle task names can arrive with a trailing non-breaking space (e.g.
    'Project Management\xa0'). Replicon stores task names trimmed, so we must send them
    trimmed too - otherwise PutTask's exact-name lookup misses the already-created task,
    falls into the create branch, and 400s with 'The specified Task already exists.'
    """
    return (name or '').replace('\xa0', ' ').strip()


def _build_parent_chain(parent_id, task_by_id, _seen=None):
    """Nested `{name, parent: {...}}` ancestor chain for a task, by cleaned name.

    PutTask can only resolve a nested parent (a level >= 2 task) when target.parent carries the
    WHOLE chain up to the root, each level nested, ending in `parent: null` - a single immediate
    name 400s with 'Project and Task's Project mis-match'.

    Terminates as None when parent_id isn't one of this project's tasks (the Oracle project-root
    pseudo-id), which is exactly how a level-1 task gets `parent: null`. `_seen` guards against a
    malformed self/cyclic ParentTaskId.
    """
    _seen = _seen or set()
    node = task_by_id.get(parent_id)
    if not node or parent_id in _seen:
        return None
    return {
        'name': _clean_task_name(node.get('TaskName')),
        'parent': _build_parent_chain(node.get('ParentTaskId'), task_by_id, _seen | {parent_id}),
    }


def build_ordered_task_worklist(tasks):
    """Flatten the whole Oracle WBS into one parent-before-child ordered worklist.

    Replaces the old level-by-level ForEach + cross-iteration parent-uri accumulator (which
    only ever created level 1, because a ForEach body task's rail.result('self') sees only its
    own previous iteration - not a shared accumulator). Instead we resolve each task's parent
    by NAME (Task Name is unique per spec, and PutTask upserts by target.name / resolves
    target.parent by name), so no Replicon uris need to be threaded between levels.

    Sorting by TaskLevel ascending guarantees every parent is upserted before its children in
    the single RepliconServiceCallForEachItemOperator pass that consumes this list. Depth is
    never hardcoded - a 2-level or a 5-level project both just sort and flow through.

    Args:
        tasks: flat list of Oracle task dicts (TaskId, TaskName, TaskLevel, ParentTaskId, ...).

    Returns:
        list[dict]: each input task with a cleaned 'TaskName' plus 'parent_chain' - the nested
        `{name, parent: {...}}` ancestor chain (None at level 1) that PutTask's target.parent needs.
    """
    tasks = tasks or []
    task_by_id = {task.get('TaskId'): task for task in tasks}

    worklist = []
    for task in sorted(tasks, key=_task_level):
        worklist.append({
            **task,
            'TaskName': _clean_task_name(task.get('TaskName')),
            'parent_chain': _build_parent_chain(task.get('ParentTaskId'), task_by_id),
        })
    return worklist


def flatten_replicon_tasks(data):
    """Flatten a TaskService1.svc/GetDescendantTaskDetails response into a flat task list.

    Reimplements rail.GetAllProjectTasksOperator's _flatten_response/_flatten_recurse as a plain
    data_handler - that operator subclass hardcodes endpoint/data/data_handler in its own
    __init__, which is incompatible with BatchTaskRunOperator's task-clone-and-reconstruct step
    (see get_replicon_tasks in process_project_child.py for the "multiple values for keyword
    argument 'endpoint'" this caused). Behaviour is identical: strip the duplicated nested
    'project' field, convert timeEntryDateRange dates from {year,month,day} to 'YYYY-MM-DD', and
    flatten the task + childTasks tree into one flat list.
    """
    def convert_date(date):
        if not date:
            return None
        return f"{date['year']:04}-{date['month']:02}-{date['day']:02}"

    def recurse(node, result):
        task = node['task']
        del task['project']
        if task['timeEntryDateRange']:
            task['timeEntryDateRange']['startDate'] = convert_date(task['timeEntryDateRange']['startDate'])
            task['timeEntryDateRange']['endDate'] = convert_date(task['timeEntryDateRange']['endDate'])
        result.append(task)
        for child in node['childTasks']:
            recurse(child, result)

    result = []
    for task in (data or []):
        recurse(task, result)
    return result


# ---------------------------------------------------------------------------
# Watermark math (plain Airflow Variable, no RAIL sync-time operator)
# ---------------------------------------------------------------------------

def build_replicon_task_index(replicon_tasks):
    """Map a Replicon task code (== Oracle TaskNumber) to its task uri and name."""
    index = {}
    for task in (replicon_tasks or []):
        code = task.get('code')
        uri = task.get('uri')
        if code and uri:
            index[str(code)] = {'uri': uri, 'name': task.get('name')}
    return index


def prepare_resourcing(assignments, replicon_tasks):
    """Build resource-group OEF items and role-placeholder items per Replicon task.

    Args:
        assignments: normalised planning assignments (from response_filter).
        replicon_tasks: tasks read back from Replicon (code + uri + name).

    Returns:
        dict with 'rg_oef_items' [{task_uri, task_name, value}] and
        'placeholder_items' [{task_uri, task_number, resource_name, planned_qty}].
    """
    task_by_code = build_replicon_task_index(replicon_tasks)

    grouped = {}
    for assignment in (assignments or []):
        code = str(assignment.get('task_number') or '')
        grouped.setdefault(code, []).append(assignment)

    rg_oef_items = []
    placeholder_items = []
    placeholder_oef_items = []
    for code, task_assignments in grouped.items():
        task = task_by_code.get(code)
        if not task:
            continue
        task_uri = task['uri']
        task_name = task['name']

        oef_value = resource_groups_oef_value(task_assignments)
        if oef_value:
            rg_oef_items.append({'task_uri': task_uri, 'task_name': task_name, 'value': oef_value})

        seen = set()
        ph_names = []
        for assignment in task_assignments:
            if not belongs_as_individual_placeholder(assignment):
                continue
            name = (assignment.get('resource_name') or '').strip()
            if name.lower() in seen:
                continue
            seen.add(name.lower())
            ph_names.append(name)
            placeholder_items.append({
                'task_uri': task_uri,
                'task_number': code,
                'resource_name': name,
                'planned_qty': assignment.get('planned_qty'),
            })

        if ph_names:
            placeholder_oef_items.append({
                'task_uri': task_uri,
                'task_name': task_name,
                'value': ' | '.join(sorted(ph_names)),
            })

    return {
        'rg_oef_items': rg_oef_items,
        'placeholder_items': placeholder_items,
        'placeholder_oef_items': placeholder_oef_items,
    }


def extract_roles_from_response(response):
    """Extract the roles list from a ProjectRoleService1.svc/GetActiveRoles response.

    Same shape/precedent as the sandtechinc user_import DAG's use of this endpoint: a plain
    REST list response wrapped as {'d': [...]}, each role a dict with 'displayText' and 'uri'.
    """
    data = response.json()
    if isinstance(data, dict) and 'd' in data:
        return data['d']
    if isinstance(data, list):
        return data
    return []


def all_task_uris(replicon_tasks):
    """All task URIs in the project (from get_replicon_tasks), regardless of whether Oracle
    currently has a placeholder assignment for them - the orphan-cleanup task universe must
    include tasks whose LAST placeholder was removed from Oracle entirely, not just tasks
    still present in placeholder_items."""
    return [t['uri'] for t in (replicon_tasks or []) if t.get('uri')]


def distinct_task_uris(placeholder_items):
    """Deduplicate task URIs from placeholder items, preserving first-seen order.

    Used by both the cleanup-phase RepliconServiceCallForEachItemOperator (items=) and the
    orphan-diff function (for result matching), so they stay positionally aligned.
    """
    seen = set()
    result = []
    for item in (placeholder_items or []):
        task_uri = item.get('task_uri')
        if task_uri and task_uri not in seen:
            seen.add(task_uri)
            result.append(task_uri)
    return result


def find_orphaned_resource_estimates(placeholder_items, active_roles, task_uris, task_estimates_responses):
    """Estimates on Replicon tasks whose role no longer exists as an Oracle placeholder.

    Never deletes an estimate that already has a resourceUser assigned — once a PM staffs
    a placeholder, Polaris owns those hours (same rule as estimate_is_updatable).

    Args:
        placeholder_items: Oracle-derived placeholder list from prepare_resourcing().
        active_roles: GetActiveRoles REST response (list of dicts with 'displayText', 'uri').
        task_uris: Deduplicated list of task URIs (from distinct_task_uris()).
        task_estimates_responses: Parallel list of GraphQL responses from
            get_task_estimates_for_cleanup, one per task_uri.

    Returns:
        List of dicts [{'taskId': task_uri, 'taskResourceEstimateId': estimate_id}, ...] —
        each represents an orphaned estimate ready for deletion.
    """
  
    role_uri_by_name = {
        (r.get('displayText') or '').strip().lower(): r.get('uri')
        for r in (active_roles or []) if r.get('uri')
    }

    valid_keys = set()
    for item in (placeholder_items or []):
        role_name = (item.get('resource_name') or '').strip().lower()
        role_uri = role_uri_by_name.get(role_name)
        if role_uri:
            valid_keys.add((item['task_uri'], role_uri))

    orphans = []
    for task_uri, response in zip(task_uris or [], task_estimates_responses or []):
        task_data = ((response or {}).get('data') or {}).get('task') or {}
        for estimate in (task_data.get('resourceEstimates') or []):
            role_uri = (estimate.get('projectRole') or {}).get('id')
            if (task_uri, role_uri) in valid_keys:
                continue
            if (estimate.get('resourceUser') or {}).get('id'):
                continue
            orphans.append({
                'taskId': task_uri,
                'taskResourceEstimateId': estimate['id']
            })
    return orphans


def find_role_uri_by_name(active_roles, role_name):
    """Find a role uri by case-insensitive name from a GetActiveRoles REST response list."""
    target = (role_name or '').strip().lower()
    for role in (active_roles or []):
        if (role.get('displayText') or '').strip().lower() == target:
            return role.get('uri')
    return None


# ---------------------------------------------------------------------------
# Run-log report helpers (log-generation child DAG)
# ---------------------------------------------------------------------------

def _status_is(properties, status):
    return (properties.get('status') or '').strip().lower() == status.lower()


def get_record_count_by_status(dag_run):
    """Count log entries by status for the run-report email subject + body.

    Reads the shared run log (SQLite artifact) via rail.load_all_records and stashes
    error/exception counts as results so the email subject template can branch on them.
    """
    log_artifact = dag_run.conf.get('log')
    records = rail.load_all_records(log_artifact) if log_artifact else []

    def count(status):
        return len([r for r in records if _status_is(r.get('properties', {}), status)])

    counts = {
        'total_record_count': len(records),
        'success_record_count': count('success'),
        'error_record_count': count('error'),
        'exception_record_count': count('exception'),
    }
    rail.set_result(key='error_record_count', val=counts['error_record_count'])
    rail.set_result(key='exception_record_count', val=counts['exception_record_count'])
    return counts


def get_log_email_details(timezone, dag_run):
    """Timestamps + generated CSV file name for the run-report email."""
    current = now(timezone)
    stamp = current.strftime('%Y%m%dT%H%M%S')
    return {
        'start_time': dag_run.conf.get('start_time'),
        'job_end_time': current.strftime('%Y-%m-%d %H:%M:%S %Z'),
        'log_timestamp': stamp,
        'log_file_name': f'{config.log_file_name_prefix}_{stamp}.csv',
    }


def compute_query_watermark(stored_value, now_dt):
    """Lower bound for the Oracle delta query.

    Uses the stored watermark when present, otherwise falls back to
    now - WATERMARK_INITIAL_LOOKBACK_HOURS on the first run.

    Args:
        stored_value: previously persisted watermark string (or falsy on first run).
        now_dt: timezone-aware datetime for "now".

    Returns:
        str: an Oracle-formatted timestamp to use in the delta filter.
    """
    if stored_value:
        return stored_value
    lookback = now_dt - timedelta(hours=config.WATERMARK_INITIAL_LOOKBACK_HOURS)
    return lookback.strftime(config.WATERMARK_DATE_FORMAT)

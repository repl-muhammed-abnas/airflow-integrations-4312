
from datetime import timedelta
import uuid
from airflow.models import Variable
import rail

null = None


# Maps Costpoint Project Role codes to Replicon PM ('pm') or Co-Manager ('co').
# Project Roles takes precedence; if no 'pm' role is assigned on a project,
# the legacy EMPL_ID from PJMBASIC_PROJ is used as a fallback.


def _extract_role_assignments(role_rows):
    """Resolve PJMROLE response into a list of role assignments by EMPL_ID.

    The role method returns three sibling rowsets under PJMROLE_HDR:
      - PJMROLE_PROJROLEUSER_CHILD: {USER_ID, ROLE_CD} - actual assignments
      - PJMROLE_USERS_CHILD:        {PJMROLE_USERS_CHILD_USER_ID, EMPL_ID, NAME} - user catalog
      - PJMROLE_ROLE_CHILD:         {PJMROLE_ROLE_CHILD_ROLE_CD, ROLE_DESC}      - role catalog
    Assignments carry USER_ID (login), not EMPL_ID. Resolve via the user catalog.
    Returns (resolved, skipped). `skipped` lists USER_IDs that have no EMPL_ID
    in CP - those cannot be mapped to a Polaris user and must be surfaced as Errors.
    """
    user_to_empl = {}
    assignments = []
    for hdr in role_rows or []:
        row = hdr.get('row') or {}
        proj_id = (row.get('data') or {}).get('PROJ_ID')
        for child in row.get('children') or []:
            crow = child.get('row') or {}
            rs = crow.get('rsId')
            data = crow.get('data') or {}
            if rs == 'PJMROLE_USERS_CHILD':
                uid = data.get('PJMROLE_USERS_CHILD_USER_ID')
                emp = data.get('EMPL_ID')
                if uid:
                    user_to_empl[uid] = emp  # may be None
            elif rs == 'PJMROLE_PROJROLEUSER_CHILD':
                assignments.append({
                    'proj_id': proj_id,
                    'user_id': data.get('USER_ID'),
                    'role_code': data.get('ROLE_CD'),
                })
    resolved, skipped = [], []
    for a in assignments:
        emp = user_to_empl.get(a['user_id'])
        if emp:
            resolved.append({'emp_id': emp, 'role_code': a['role_code'], 'proj_id': a['proj_id']})
        else:
            skipped.append({
                'user_id': a['user_id'],
                'role_code': a['role_code'],
                'reason': 'CP user has no EMPL_ID',
            })
    return resolved, skipped


def _flatten_task_tree(nodes, project_flag_map=None, level_map=None, parent = ''):
    """Recursively flatten a nested task tree into a flat list of task dicts."""
    project_flag_map = project_flag_map or {}
    level_map = level_map or {}
    result = []
    for node in (nodes or []):
        task = node.get('task') or {}
        task_code = task.get('code')
        task_hierarchy = parent + "/" + task_code if parent else task_code
        result.append({
            'taskUri': task.get('uri'),
            'taskCode': task_code,
            'taskCodeHierarchy': task_hierarchy,
            'project_work_force_flag': project_flag_map.get(task_code),
            'level_number': level_map.get(task_code),
            'taskName': task.get('name')
        })
        result.extend(_flatten_task_tree(
            node.get('childTasks') or [], project_flag_map, level_map, task_hierarchy))
    return result


def _build_task_tree_from_task_list(data):
    """Convert a TaskListService1.svc/GetData response into a nested task tree
    compatible with _flatten_task_tree().

    The GetData response is a flat list of rows where each task references its
    parent through the 'parent' column. This rebuilds the parent/child tree so
    the same flattening logic used for GetDescendantTaskDetails can be reused.

    RAIL's default response handler already unwraps the top-level 'd' envelope,
    so ``data`` is normally ``{'header': [...], 'rows': [...]}``. The 'd' lookup
    below keeps this robust if a still-wrapped response is ever passed in.
    """
    d = data or {}
    if isinstance(d, dict):
        d = d.get('d', d)
    d = d or {}
    headers = d.get('header') or []
    rows = d.get('rows') or []
    col_index = {h.get('uri'): i for i, h in enumerate(headers)}
    task_col = col_index.get('urn:replicon:task-list-column:task')
    code_col = col_index.get('urn:replicon:task-list-column:code')
    name_col = col_index.get('urn:replicon:task-list-column:name')
    parent_col = col_index.get('urn:replicon:task-list-column:parent')

    def cell(cells, idx):
        return cells[idx] if idx is not None and idx < len(cells) else {}

    nodes_by_uri = {}
    parent_links = []
    for row in rows:
        cells = row.get('cells') or []
        task_uri = cell(cells, task_col).get('uri')
        if not task_uri:
            continue
        nodes_by_uri[task_uri] = {
            'task': {
                'uri': task_uri,
                'code': cell(cells, code_col).get('textValue'),
                'name': cell(cells, name_col).get('textValue'),
            },
            'childTasks': []
        }
        parent_links.append((task_uri, cell(cells, parent_col).get('uri')))

    roots = []
    for task_uri, parent_uri in parent_links:
        node = nodes_by_uri[task_uri]
        parent_node = nodes_by_uri.get(parent_uri) if parent_uri else None
        if parent_node is not None:
            parent_node['childTasks'].append(node)
        else:
            roots.append(node)
    return roots


def _flatten_task_list_response(data, project_flag_map=None):
    """Flatten a TaskListService1.svc/GetData response into a flat task list.

    The task code hierarchy is built from the parent chain of codes and the
    level number is derived from the task code's WBS depth (the number of
    '.'-separated segments), so PLC role tasks that share their parent's code
    keep the parent's level. The work force flag is looked up from the supplied
    Costpoint map keyed by project/task code.
    """
    project_flag_map = project_flag_map or {}
    result = []

    def _walk(nodes, parent_hierarchy):
        for node in (nodes or []):
            task = node.get('task') or {}
            task_code = task.get('code')
            task_hierarchy = (parent_hierarchy + "/" + task_code) if parent_hierarchy else task_code
            result.append({
                'taskUri': task.get('uri'),
                'taskCode': task_code,
                'taskCodeHierarchy': task_hierarchy,
                'project_work_force_flag': project_flag_map.get(task_code),
                'level_number': len(task_code.split('.')) if task_code else None,
                'taskName': task.get('name'),
            })
            _walk(node.get('childTasks') or [], task_hierarchy)

    _walk(_build_task_tree_from_task_list(data), '')
    return result


def _active_status_uri(udf_list_key='task_udfs'):
    """Resolve the 'Costpoint Active Status' UDF URI from dag_run.conf.

    Returns the URI, or None when the UDF isn't defined. Callers resolve it
    once and reference it twice (customField.uri and the conditional-include
    guard) to avoid scanning the udf list twice per task/project node.
    """
    return rail.find_first_by_attr_and_get_attr(
        (rail.get_dag_run_conf() or {}).get(udf_list_key, []),
        'textValue', 'Costpoint Active Status', 'uri'
    )


def _compute_closable_ids(rows):
    """Return frozenset of Costpoint PROJ_IDs whose row AND every descendant have
    ACTIVE_FL == 'N'. Used to decide whether a Polaris task/root should be closed.

    Parent/child links are inferred from PROJ_ID prefix (each child PROJ_ID is
    '<parent>.<segment>' where the parent PROJ_ID exists in the same result set).
    Rows missing PROJ_ID or ACTIVE_FL fail-safe to NOT closable (leaves the
    Polaris task open rather than closing something we can't reason about).
    """
    if not rows:
        return frozenset()
    by_id = {}
    for r in rows:
        pid = r.get('PROJ_ID')
        if pid and pid not in by_id:
            by_id[pid] = r
    children_by_parent = {}
    for pid in by_id:
        if '.' in pid:
            parent_pid = pid.rsplit('.', 1)[0]
            if parent_pid in by_id:
                children_by_parent.setdefault(parent_pid, []).append(pid)

    memo = {}

    def closable(pid):
        if pid in memo:
            return memo[pid]
        r = by_id.get(pid)
        if not r or r.get('ACTIVE_FL') != 'N':
            memo[pid] = False
            return False
        for child_pid in children_by_parent.get(pid, []):
            if not closable(child_pid):
                memo[pid] = False
                return False
        memo[pid] = True
        return True

    return frozenset(pid for pid in by_id if closable(pid))


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_polaris_project_sync_child_{config.instance}',
        description=f'deltek_costpoint_polaris_project_sync_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        },
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        def _is_eligible(project_info):
            """Return True when the Costpoint project row meets sync-eligibility criteria.

            Eligibility uses an *exclusion* list of TC_PROJ_FL codes sourced from the
            instance config (``config.excluded_project_type_flags``) so tenants can
            adjust the disallowed codes without changing this DAG.

            A row is also excluded when its project type (``PROJ_TYPE_DC``) is listed
            in ``config.project_type_exclusions`` OR its project classification
            (``S_PROJ_RPT_DC``) is listed in ``config.project_classification_exclusions``.
            Both lists are optional; unset lists mean no exclusion (backwards compatible).
            """
            if not project_info:
                return False
            return (
                project_info.get('ACTIVE_FL') == 'Y'
                and project_info.get('ALLOW_CHARGES_FL') == 'Y'
                and project_info.get('TC_PROJ_FL') not in config.excluded_project_type_flags
                and not _is_type_or_classification_excluded(project_info)
            )

        def _has_exclusions():
            """True when this instance configures any project type/classification
            exclusion list. Single source of truth — reused by _root_active,
            check_should_create, and the status/isTimeEntryAllowed logic so that
            adding a third exclusion list only requires updating config.py."""
            return bool(
                (getattr(config, 'project_type_exclusions', None) or [])
                or (getattr(config, 'project_classification_exclusions', None) or [])
            )

        def _is_type_or_classification_excluded(project_info):
            """True when the row's project type (``PROJ_TYPE_DC``) is in
            ``project_type_exclusions`` OR its classification (``S_PROJ_RPT_DC``) is in
            ``project_classification_exclusions``. Both lists are optional; unset lists
            mean the row is never excluded (backwards compatible)."""
            if not project_info:
                return False
            type_excl = getattr(config, 'project_type_exclusions', None) or []
            class_excl = getattr(config, 'project_classification_exclusions', None) or []
            return (project_info.get('PROJ_TYPE_DC') in type_excl
                    or project_info.get('S_PROJ_RPT_DC') in class_excl)

        def _retained_proj_ids(rows, root_id):
            if not rows:
                return set()
            keep = set()
            root_id = str(root_id or '')
            # Resolve exclusion once per row (O(n)) so the inner loop is string-only.
            included_ids = [
                str(r.get('PROJ_ID', ''))
                for r in rows
                if not _is_type_or_classification_excluded(r)
            ]
            # Root rescued a fully-excluded tree would keep it In Progress; that is the
            # Archived path. Root is a valid seed only when it has no children at
            # all (single-row project). Non-root nodes are always eligible as seeds.
            root_has_children = any(
                str(r.get('PROJ_ID', '')).startswith(root_id + '.') for r in rows
            )
            for proj_id in included_ids:
                prefix = proj_id + '.'
                if any(other.startswith(prefix) for other in included_ids):
                    continue  # has an inclusion descendant — deeper node is the seed
                if proj_id == root_id and root_has_children:
                    continue  # root with only excluded children
                parts = proj_id.split('.')
                for i in range(1, len(parts) + 1):
                    keep.add('.'.join(parts[:i]))
            return keep

        def _require_chargeable_leaf():
            """Single validated read of require_chargeable_leaf_in_hierarchy for this run."""
            val = getattr(config, 'require_chargeable_leaf_in_hierarchy', True)
            if not isinstance(val, bool):
                raise ValueError(
                    f"require_chargeable_leaf_in_hierarchy must be True or False, got: {val!r}"
                )
            return val

        def _root_active(root_project_info):
            """Return True when the root/boundary project should remain Active in Polaris.

            Decision tree (evaluated top-to-bottom):

            1. Exclusion lists configured (``project_type_exclusions`` or
               ``project_classification_exclusions`` non-empty):
               - If the root row itself matches an exclusion → False (Archived).
               - Otherwise → True only when at least one non-excluded leaf survives
                 anywhere beneath the root (``_retained_proj_ids`` non-empty).
               This is the MAP2-3473 deactivation-on-exclusion-match path and runs
               regardless of ``require_chargeable_leaf_in_hierarchy``.

            2. ``require_chargeable_leaf_in_hierarchy`` is False (exclusions absent):
               - Root must be ACTIVE_FL='Y' and TC_PROJ_FL not in
                 ``excluded_project_type_flags``; ALLOW_CHARGES_FL is not consulted.

            3. Default (require_chargeable True, no exclusions):
               - Preserves pre-existing behaviour: delegates to ``_is_eligible`` which
                 requires ACTIVE_FL, ALLOW_CHARGES_FL, and valid TC_PROJ_FL."""
            if _has_exclusions():
                if root_project_info and _is_type_or_classification_excluded(root_project_info):
                    return False
                return bool(_cached_retained_proj_ids())
            if not _require_chargeable_leaf():
                if not root_project_info:
                    return False
                return (
                    root_project_info.get('ACTIVE_FL') == 'Y'
                    and root_project_info.get('TC_PROJ_FL') not in config.excluded_project_type_flags
                )
            return _is_eligible(root_project_info)

        # Per-run memoised set of Costpoint PROJ_IDs that should be closed in
        # Polaris (self + every descendant is ACTIVE_FL='N'). Also gates the
        # root-project archival decision below.
        _closable_memo = {}

        def _closable_ids():
            if 'set' not in _closable_memo:
                _closable_memo['set'] = _compute_closable_ids(get_project_data()[1])
            return _closable_memo['set']

        # Per-run memoised retained-PROJ_ID set. _retained_proj_ids is O(n²) over
        # the WBS tree (each _is_leaf call scans all rows); caching avoids
        # recomputing on every call within the same child-DAG run.
        _retained_memo = {}

        def _cached_retained_proj_ids():
            if 'set' not in _retained_memo:
                root_project_id, data, _ = get_project_data()
                _retained_memo['set'] = _retained_proj_ids(data, root_project_id)
            return _retained_memo['set']

        def _closed_flag(proj_id):
            """Return 'true'/'false' for the isClosed payload field."""
            return "true" if proj_id in _closable_ids() else "false"

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id="create_log",
        )

        get_project_basic_details = rail.RepliconServiceOperator(
            task_id='get_project_basic_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails2",
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": "{{ dag_run.conf.item.root_project_id }}",
                        "parameterCorrelationId": null
                    }
                ]
            },
            data_handler=lambda data: (data[0].get('projectDetails') if data else null)
        )

        pick_chose_wbs_sync = rail.IfOperator(
            task_id='pick_chose_wbs_sync',
            test=lambda: bool(getattr(config, 'allow_only_chargeable', False)) or bool(getattr(config, 'enable_wbs_boundary_sync', False)),
            yes_task='get_costpoint_projects_from_conf',
            no_task='get_costpoint_projects'
        )

        get_costpoint_projects_from_conf = rail.PythonOperator(
            task_id='get_costpoint_projects_from_conf',
            python_callable=lambda: rail.get_dag_run_conf().get('item', {}).get('data', [])
        )

        def get_costpoint_projects_data():
            if bool(getattr(config, 'allow_only_chargeable', False)) or bool(getattr(config, 'enable_wbs_boundary_sync', False)):
                return rail.get_dag_run_conf().get('item', {}).get('data', [])
            return rail.result('get_costpoint_projects')

        get_costpoint_projects = rail.DeltekCostPointServiceOperator(
            task_id='get_costpoint_projects',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company="{{ dag_run.conf.item.data[0] | attr_or_default('_company') | sn }}",
            data={
                "filter": {
                    "id": "polaris_exp_project",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJMBASIC_PROJ",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "PROJ_ID",
                                                "relation": "like%",
                                                "value": "{{ dag_run.conf.item.root_project_id }}"
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: data['document']['rows'],
        )

        get_existing_client = rail.RepliconServiceOperator(
            task_id='get_existing_client',
            endpoint="/services/ClientListService1.svc/GetData",
            data=lambda: {
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:client-list-column:client"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:client-list-filter:name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": (get_project_data()[2] or {}).get('CUST_NAME')
                        },
                    }
                }
            },
            data_handler=lambda data: list(filter(
                lambda name: name == (get_project_data()[2] or {}).get('CUST_NAME'),
                map(lambda x: x['cells'][0]['textValue'], data['rows'])))
        )

        # No client name on the project -> nothing to create; skip straight to
        # workforce. Otherwise create the client only when it doesn't already exist.
        is_client_exists = rail.IfOperator(
            task_id='is_client_exists',
            test=lambda: bool(rail.result('get_existing_client')) or not (get_project_data()[2] or {}).get('CUST_NAME'),
            yes_task='get_workforce_user_costpoint',
            no_task='create_root_project_client'
        )

        create_root_project_client = rail.RepliconServiceOperator(
            task_id='create_root_project_client',
            endpoint="/services/ClientService1.svc/PutClient",
            data=lambda: {
                "client": {
                    "target": {
                        "uri": null,
                        "name": (get_project_data()[2] or {}).get('CUST_NAME'),
                        "code": null,
                        "parameterCorrelationId": null
                    },
                    "name": (get_project_data()[2] or {}).get('CUST_NAME'),
                    "code": null,
                    "comment": null,
                    "clientManager": null,
                    "billingContact": null,
                    "clientAddress": null,
                    "billingAddress": null,
                    "isActive": "true",
                    "customFieldValues": [],
                    "billingRates": [],
                    "expenseCodesAllowedByDefaultOnNewProjects": [],
                    "defaultBillingCurrency": null
                }
            }
        )

        get_workforce_user_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_workforce_user_costpoint',
            trigger_rule='none_failed_min_one_success',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company="{{ dag_run.conf.item.data[0] | attr_or_default('_company') | sn }}",
            data=lambda: {
                "filter": {
                    "id": "polaris_exp_project_workforce",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJM_PROJEMPL_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "PROJ_ID",
                                                "relation": "like%",
                                                "value": get_project_data()[0]
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: data['document']['rows'],
        )

        def map_workforce_billingrates():
            billing_rates = []
            seen = set()
            for row in (rail.result('get_workforce_user_costpoint') or []):
                project_id = str(((row.get('row') or {}).get(
                    'data') or {}).get('PROJ_ID') or '').strip()
                employees_with_role = set()
                childto_emp_ids = []
                for child in (row.get('row') or {}).get('children', []):
                    child_rs_id = (child.get('row') or {}).get('rsId')
                    if child_rs_id == 'PJM_PROJEMPL_LABCAT_PLCWKFRCE':
                        for entry in (child.get('row') or {}).get('children', []):
                            data = (entry.get('row') or {}).get('data', {})
                            employee_id = data.get(
                                'PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID')
                            role = data.get(
                                'PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD')
                            if employee_id and role:
                                employees_with_role.add(employee_id)
                                key = (project_id, employee_id, role)
                                if key not in seen:
                                    seen.add(key)
                                    billing_rates.append({
                                        'projectid': project_id,
                                        'employeeId': employee_id,
                                        'role': role,
                                    })
                    elif child_rs_id == 'PJM_PROJEMPL_CHILDTO':
                        emp_id = (child.get('row') or {}).get(
                            'data', {}).get('EMPL_ID')
                        if emp_id:
                            childto_emp_ids.append(emp_id)
                # Include project workforce employees (PJM_PROJEMPL_CHILDTO) that
                # have no PLC/billing role, keeping each
                # (projectid, employeeId) pair unique.
                for emp_id in childto_emp_ids:
                    if emp_id in employees_with_role:
                        continue
                    key = (project_id, emp_id, None)
                    if key not in seen:
                        seen.add(key)
                        billing_rates.append({
                            'projectid': project_id,
                            'employeeId': emp_id,
                            'role': None,
                        })
            return billing_rates

        get_billing_rates_costpoint = rail.PythonOperator(
            task_id='get_billing_rates_costpoint',
            python_callable=map_workforce_billingrates
        )

        def do_user_data_handler(data):
            # Align response to requested employeeIds by content, not position.
            # BulkGetUsers2 typically returns same-length array with empty slots
            # for missing users, but using positional emp_ids.index(x) lookups
            # is fragile if Replicon ever reorders or drops slots. Build an
            # employeeId -> userDetails map by reading the response's own
            # employeeId field, then assemble in the order of map_workforce_empid().
            emp_ids = map_workforce_empid()
            details_by_emp_id = {}
            for item in (data or []):
                if not item:
                    continue
                emp_id = item.get('employeeId') or (item.get('userDetails') or {}).get('employeeId')
                if emp_id and emp_id not in details_by_emp_id:
                    details_by_emp_id[emp_id] = item
            result = []
            for emp_id in emp_ids:
                # Fallback to positional lookup if the response item didn't
                # echo back the employeeId — keeps prior behavior intact.
                details = details_by_emp_id.get(emp_id)
                if details is None and data:
                    try:
                        details = data[emp_ids.index(emp_id)]
                    except (IndexError, ValueError):
                        details = None
                result.append({"employeeId": emp_id, 'userDetails': details})
            return result

        def map_workforce_empid():
            emp_ids = set()
            for row in (rail.result('get_workforce_user_costpoint') or []):
                for child in (row.get('row') or {}).get('children', []):
                    child_rs_id = (child.get('row') or {}).get('rsId')
                    if child_rs_id == 'PJM_PROJEMPL_LABCAT_PLCWKFRCE':
                        for entry in (child.get('row') or {}).get('children', []):
                            emp_id = (entry.get('row') or {}).get('data', {}).get(
                                'PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID')
                            if emp_id:
                                emp_ids.add(emp_id)
                    elif child_rs_id == 'PJM_PROJEMPL_CHILDTO':
                        emp_id = (child.get('row') or {}).get(
                            'data', {}).get('EMPL_ID')
                        if emp_id:
                            emp_ids.add(emp_id)
            return list(emp_ids)

        get_users_from_replicon = rail.RepliconServiceOperator(
            task_id='get_users_from_replicon',
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data=lambda: {
                "users": list(map(lambda x: {"employeeId": x}, map_workforce_empid()))

            },
            data_handler=do_user_data_handler
        )

        is_project_role_assigment_enabled = rail.IfOperator(
            task_id='is_project_role_assigment_enabled',
            test=lambda: bool(getattr(config, 'is_project_role_assigment_enabled', False)),
            yes_task='get_project_roles_costpoint',
            no_task='get_project_leader_info_from_replicon'
        )

        get_project_leader_info_from_replicon = rail.RepliconServiceOperator(
            task_id='get_project_leader_info_from_replicon',
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data=lambda: {
                "users": [{"employeeId": get_project_data()[2].get('EMPL_ID')}]
            },
            data_handler=lambda data: data[0] if data else None
        )

        get_project_roles_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_project_roles_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company="{{ dag_run.conf.item.data[0] | attr_or_default('_company') | sn }}",
            data=lambda: {
                "filter": {
                    "id": "polaris_exp_project_roles",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJMROLE_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "PROJ_ID",
                                                "relation": "=",
                                                "value": rail.get_dag_run_conf()['item']['root_project_id']
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: data['document']['rows'],
        )

        def do_get_pm_comanager_candidates():
            pm_map = config.CP_ROLE_TO_PM_MAP or {}
            root_project_id = rail.get_dag_run_conf()['item']['root_project_id']
            role_rows = rail.result('get_project_roles_costpoint') or []
            assignments, missing_empl_id = _extract_role_assignments(role_rows)
            assignments = [
                a for a in assignments
                if a.get('proj_id') in (None, root_project_id)
                and a.get('role_code') in pm_map
            ]
            missing_empl_id = [m for m in missing_empl_id if m.get('role_code') in pm_map]
            candidates = [
                {
                    'emp_id': a['emp_id'],
                    'role_code': a['role_code'],
                    'type': pm_map.get(a['role_code']),
                }
                for a in assignments
            ]
            return {'candidates': candidates, 'missing_empl_id': missing_empl_id}

        get_pm_comanager_candidates = rail.PythonOperator(
            task_id='get_pm_comanager_candidates',
            python_callable=do_get_pm_comanager_candidates,
        )

        def do_pm_comanager_users_handler(data):
            candidates = (rail.result('get_pm_comanager_candidates') or {}).get('candidates', [])
            details_by_emp_id = {}
            for api_item in (data or []):
                details = (api_item or {}).get('userDetails') or {}
                emp_id = details.get('employeeId')
                if emp_id and emp_id not in details_by_emp_id:
                    details_by_emp_id[emp_id] = details
            result = []
            for c in candidates:
                result.append({
                    'emp_id': c['emp_id'],
                    'role_code': c['role_code'],
                    'type': c['type'],
                    'userDetails': details_by_emp_id.get(c['emp_id']),
                })
            return result

        get_pm_comanager_users = rail.RepliconServiceOperator(
            task_id='get_pm_comanager_users',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": c['emp_id'],
                        "parameterCorrelationId": null
                    }
                    for c in (rail.result('get_pm_comanager_candidates') or {}).get('candidates', [])
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=do_pm_comanager_users_handler
        )

        def do_resolve_pm_comanagers():
            cand_result = rail.result('get_pm_comanager_candidates') or {}
            missing_empl_id = cand_result.get('missing_empl_id') or []
            base_skips = [
                {
                    'emp_id': '',
                    'user_id': m.get('user_id'),
                    'role_code': m.get('role_code'),
                    'reason': m.get('reason') or 'CP user has no EMPL_ID',
                }
                for m in missing_empl_id
            ]
            users = rail.result('get_pm_comanager_users') or []
            if not users:
                return {'pm_emp_id': None, 'pm_user_uri': None,
                        'co_manager_user_uris': [], 'skipped': base_skips}
            pm_emp_id, pm_user_uri = None, None
            co_manager_user_uris = []
            skipped = list(base_skips)
            # Pass 1: resolve PM (first valid 'pm' candidate wins) so the
            # co-manager pass can exclude that URI regardless of candidate order.
            # This also guards against the same user being assigned to both a
            # PM-mapped role (e.g. LEM) and a co-manager-mapped role in CP.
            for u in users:
                if u['type'] != 'pm':
                    continue
                user_uri = (u.get('userDetails') or {}).get('uri')
                if user_uri and not pm_user_uri:
                    pm_emp_id = u['emp_id']
                    pm_user_uri = user_uri
                else:
                    skipped.append({
                        'emp_id': u['emp_id'],
                        'role_code': u['role_code'],
                        'reason': 'user not found in Replicon' if not user_uri else 'duplicate PM skipped',
                    })
            # Pass 2: co-managers, excluding the resolved PM and de-duped.
            for u in users:
                if u['type'] == 'pm':
                    continue
                user_uri = (u.get('userDetails') or {}).get('uri')
                if not user_uri:
                    skipped.append({
                        'emp_id': u['emp_id'],
                        'role_code': u['role_code'],
                        'reason': 'user not found in Replicon',
                    })
                elif user_uri == pm_user_uri:
                    skipped.append({
                        'emp_id': u['emp_id'],
                        'role_code': u['role_code'],
                        'reason': 'user already assigned as PM; co-manager role skipped',
                    })
                elif user_uri not in co_manager_user_uris:
                    co_manager_user_uris.append(user_uri)
            return {
                'pm_emp_id': pm_emp_id,
                'pm_user_uri': pm_user_uri,
                'co_manager_user_uris': co_manager_user_uris,
                'skipped': skipped,
            }

        resolve_pm_comanagers = rail.PythonOperator(
            task_id='resolve_pm_comanagers',
            python_callable=do_resolve_pm_comanagers
        )

        def do_get_pm_comanager_uris():
            resolved = rail.result('resolve_pm_comanagers') or {}
            return [
                uri for uri in (
                    [resolved.get('pm_user_uri')] + (resolved.get('co_manager_user_uris') or [])
                ) if uri
            ]

        if_pm_uris_present = rail.IfOperator(
            task_id='if_pm_uris_present',
            test=lambda: bool(do_get_pm_comanager_uris()),
            yes_task='get_pm_comanager_current_permissions',
            no_task='get_task_list_info'
        )

        get_pm_comanager_current_permissions = rail.RepliconServiceOperator(
            task_id='get_pm_comanager_current_permissions',
            endpoint='/services/PermissionSetService1.svc/BulkGetAssignedPermissionSetsForUsers',
            data=lambda: {"userUris": do_get_pm_comanager_uris()}
        )

        def _filter_pm_uris_for_permission():
            all_uris = do_get_pm_comanager_uris()
            existing = rail.result('get_pm_comanager_current_permissions') or []
            uris_with_pm_policy = {
                (p.get('user') or {}).get('uri')
                for p in existing
                if (p.get('policyUri') or '') == 'urn:replicon:policy:project-management'
            }
            uri_to_candidate = {
                (u.get('userDetails') or {}).get('uri'): u
                for u in (rail.result('get_pm_comanager_users') or [])
                if (u.get('userDetails') or {}).get('uri')
            }
            return {
                'to_assign': [uri for uri in all_uris if uri not in uris_with_pm_policy],
                'skipped': [
                    {
                        'emp_id': (uri_to_candidate.get(uri) or {}).get('emp_id', ''),
                        'role_code': (uri_to_candidate.get(uri) or {}).get('role_code', ''),
                        'reason': 'already has project-management permission set',
                    }
                    for uri in all_uris if uri in uris_with_pm_policy
                ],
            }

        filter_pm_uris_for_permission = rail.PythonOperator(
            task_id='filter_pm_uris_for_permission',
            python_callable=_filter_pm_uris_for_permission
        )

        assign_pm_comanager_permission = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_pm_comanager_permission',
            items=lambda: (rail.result('filter_pm_uris_for_permission') or {}).get('to_assign') or [],
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda item: {
                "userUri": item,
                "permissionSetUri": rail.find_first_by_attr_and_get_attr(
                    rail.get_dag_run_conf()['permission_sets'],
                    'name',
                    config.project_manager_permission_name,
                    'uri'
                )
            }
        )

        log_pm_comanager_skips = rail.WriteLogOperator(
            task_id='log_pm_comanager_skips',
            log="{{ result('create_log') }}",
            message="na",
            severity="Error",
            items=lambda: (rail.result('resolve_pm_comanagers') or {}).get('skipped') or [],
            properties={
                "proj_id": "{{ dag_run.conf.item.root_project_id }}",
                "proj_name": "{{ dag_run.conf.item.data[0].row.data.PROJ_NAME }}",
                "action": "PM/Co-Manager Assignment",
                "status": "Error",
                "details": "Skipped EMPL_ID '{{ item.emp_id }}' USER_ID '{{ item.get('user_id','') }}' (role {{ item.role_code }}): {{ item.reason }}",
            }
        )

        log_pm_permission_skips = rail.WriteLogOperator(
            task_id='log_pm_permission_skips',
            log="{{ result('create_log') }}",
            message="na",
            severity="Info",
            items=lambda: (rail.result('filter_pm_uris_for_permission') or {}).get('skipped') or [],
            properties={
                "proj_id": "{{ dag_run.conf.item.root_project_id }}",
                "proj_name": "{{ dag_run.conf.item.data[0].row.data.PROJ_NAME }}",
                "action": "PM/Co-Manager Permission Assignment",
                "status": "Skipped",
                "details": "Skipped EMPL_ID '{{ item.emp_id }}' (role {{ item.role_code }}): {{ item.reason }}",
            }
        )

        def do_get_project_leader_uris():
            user = rail.result('get_project_leader_info_from_replicon')
            return [user['uri']] if user else []

        if_project_leader_uri_present = rail.IfOperator(
            task_id='if_project_leader_uri_present',
            test=lambda: bool(do_get_project_leader_uris()),
            yes_task='get_project_leader_current_permissions',
            no_task='get_task_list_info'
        )

        get_project_leader_current_permissions = rail.RepliconServiceOperator(
            task_id='get_project_leader_current_permissions',
            endpoint='/services/PermissionSetService1.svc/BulkGetAssignedPermissionSetsForUsers',
            data=lambda: {"userUris": do_get_project_leader_uris()}
        )

        def _filter_project_leader_uri_for_permission():
            user = rail.result('get_project_leader_info_from_replicon')
            if not user:
                return {'to_assign': [], 'skipped': []}
            existing = rail.result('get_project_leader_current_permissions') or []
            already_has = any(
                (p.get('policyUri') or '') == 'urn:replicon:policy:project-management'
                for p in existing
                if (p.get('user') or {}).get('uri') == user['uri']
            )
            if already_has:
                return {
                    'to_assign': [],
                    'skipped': [{'emp_id': user.get('employeeId', ''), 'reason': 'already has project-management permission set'}],
                }
            return {'to_assign': [user['uri']], 'skipped': []}

        filter_project_leader_uri_for_permission = rail.PythonOperator(
            task_id='filter_project_leader_uri_for_permission',
            python_callable=_filter_project_leader_uri_for_permission
        )

        log_project_leader_permission_skips = rail.WriteLogOperator(
            task_id='log_project_leader_permission_skips',
            log="{{ result('create_log') }}",
            message="na",
            severity="Info",
            items=lambda: (rail.result('filter_project_leader_uri_for_permission') or {}).get('skipped') or [],
            properties={
                "proj_id": "{{ dag_run.conf.item.root_project_id }}",
                "proj_name": "{{ dag_run.conf.item.data[0].row.data.PROJ_NAME }}",
                "action": "Project Leader Permission Assignment",
                "status": "Skipped",
                "details": "Skipped EMPL_ID '{{ item.emp_id }}': {{ item.reason }}",
            }
        )

        assign_project_leader_permission = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_project_leader_permission',
            items=lambda: (rail.result('filter_project_leader_uri_for_permission') or {}).get('to_assign') or [],
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda item: {
                "userUri": item,
                "permissionSetUri": rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['permission_sets'], 'name', config.project_manager_permission_name, 'uri')
            }
        )

        def get_assigned_resources(root_dept_uri, project_workforce_flag, proj_id, plc = None):
            result = []
            if project_workforce_flag != 'Y':
                result.extend( [
                    {
                        "department": {
                            "uri": root_dept_uri
                        }
                    }
                ]);
                if not bool(getattr(config, 'force_assign_user_resources', False)):
                    return result
            users_by_emp_id = {
                x['employeeId']: x['userDetails']
                for x in (rail.result('get_users_from_replicon') or [])
                if x.get('employeeId') and x.get('userDetails')
            }
            seen_uris = set()
            for rate in (rail.result('get_billing_rates_costpoint') or []):
                uri = (users_by_emp_id.get(
                    rate.get('employeeId')) or {}).get('uri')
                if rate['projectid'] == proj_id and uri and uri not in seen_uris and (plc is None or plc == rate['role']):
                    seen_uris.add(uri)
                    result.append({"user": {"uri": uri}})
            return result

        def get_tasks_param(data, parent_id, root_dept_uri, level_no, root_task = None):
            tasks = list(map(lambda x: get_task_details(data, x, root_dept_uri),
                        filter(
                            lambda x: x['LVL_NO'] == level_no+1 and x['PROJ_ID'].startswith(parent_id), data)))
            #add plc tasks based on config
            plc_mapping = rail.result('get_billing_rates_costpoint')

            if config.multi_plc_subtask_mode:
                #add root project plc tasks 
                if root_task:
                    plc_assignments = list(filter( lambda x: x['projectid'] == parent_id, plc_mapping))
                    get_plc_tasks_create(root_task, plc_assignments, root_dept_uri)
                for task in tasks:
                    plc_assignments =  list(filter( lambda x: x['projectid'] == task['task']['code'], plc_mapping))
                    get_plc_tasks_create(task, plc_assignments, root_dept_uri)
            return tasks
        
        def get_plc_tasks_create(task, plc_assignments, root_dept_uri):
            if plc_assignments and len(plc_assignments): 
                added_role = []
                for assignment in plc_assignments:
                    if assignment['role'] is not None and assignment['role'] not in added_role:
                        plc_task = get_plc_task(task, assignment, root_dept_uri)
                        added_role.append(assignment['role'])
                        if 'childTasks' in task :
                            task['childTasks'].append(plc_task)
                        else :
                            task['childTasks'] = [plc_task]
                task['task']['isTimeEntryAllowed'] = False

        def get_plc_task(task, assignment, root_dept_uri):
            active_status_uri = _active_status_uri()
            plc_task_name =  get_plc_task_name(assignment['role'], task['task']['name'])
            resources = get_assigned_resources(root_dept_uri, 'Y', task['task']['code'], assignment['role'])
            task_resources = []
            user_uris = []
            for resource in task['task']['assignedResources']:
                if 'user' in resource and resource['user']:
                    user_uris.append(resource['user']['uri'])
            if resources:
                for resource in resources:
                    if task['task']['assignedResources'] and resource['user'] and resource['user']['uri'] in user_uris:
                        task_resources.append(resource)

            replTask = {
                    "task": {
                        "target": {
                            'name': plc_task_name
                        },
                        "name": plc_task_name,
                        "code":  get_plc_task_code(task['task']['code']),
                        "timeEntryDateRange": {
                            "startDate": task['task']['timeEntryDateRange']['startDate'],
                            "endDate": task['task']['timeEntryDateRange']['endDate']
                        },
                        "percentCompleted": "0",
                        "isTimeEntryAllowed": task['task']['isTimeEntryAllowed'],
                        "estimatedHours": null,
                        "isClosed": _closed_flag(task['task']['code']),
                        "customFieldValues": [
                            {
                                "customField": {"uri": active_status_uri},
                                "text": null,
                                "date": null,
                                "dropDownOption": {
                                    "uri": null,
                                    "name": next(
                                        (cf['dropDownOption']['name']
                                         for cf in (task['task'].get('customFieldValues') or [])
                                         if (cf.get('dropDownOption') or {}).get('name') in ('Active', 'Inactive')),
                                        'Active'
                                    )
                                },
                                "number": null
                            }
                        ] if active_status_uri else [],
                        "extensionFieldValues": [],
                        "estimatedCost": null,
                        "costTypeUri": null,
                        "assignedResources": task_resources,
                        "timeAndMaterials": null,
                        "keyValues": [],
                        "historicalKeyValues": []
                    },
                    "childTasks": []
                }
            return replTask
        
        def get_plc_task_name(plcCode, parent_task_name):
            if not plcCode or not parent_task_name:
                return parent_task_name or ''
            return plcCode + " - " + parent_task_name
        
        def get_plc_task_code(parent_task_code):
            return parent_task_code or ''
        
        def get_task_details(data, x, root_dept_uri):
            active_status_uri = _active_status_uri()
            return {
                        "task": {
                            "target": {
                                'name':  x['PROJ_NAME'] if len(list(filter(lambda p: p['PROJ_NAME'] == x['PROJ_NAME'], data))) == 1 else f"{x['PROJ_NAME']}_{x['PROJ_ID']}"
                            },
                            "name": x['PROJ_NAME'] if len(list(filter(lambda p: p['PROJ_NAME'] == x['PROJ_NAME'], data))) == 1 else f"{x['PROJ_NAME']}_{x['PROJ_ID']}",
                            "code":  x['PROJ_ID'],
                            "description": x['PROJ_LONG_NAME'],
                            "timeEntryDateRange": {
                                "startDate": rail.parse_date(x.get('PROJ_START_DT'), config.date_time_format),
                                "endDate": rail.parse_date(x.get('PROJ_END_DT'), config.date_time_format),
                            },
                            "percentCompleted": "0",
                            "isTimeEntryAllowed": "true" if x['ALLOW_CHARGES_FL'] == 'Y' and x['ACTIVE_FL'] == 'Y' and x.get('TC_PROJ_FL') not in ('E', 'N') else "false",
                            "estimatedHours": null,
                            "isClosed": _closed_flag(x['PROJ_ID']),
                            "customFieldValues": [
                                {
                                    "customField": {"uri": active_status_uri},
                                    "text": null,
                                    "date": null,
                                    "dropDownOption": {
                                        "uri": null,
                                        "name": "Active" if _is_eligible(x) else "Inactive"
                                    },
                                    "number": null
                                }
                            ] if active_status_uri else [],
                            "extensionFieldValues": [],
                            "estimatedCost": null,
                            "costTypeUri": null,
                            "assignedResources": get_assigned_resources(root_dept_uri, x.get('PROJ_WORK_FRC_FL'), x.get('PROJ_ID')),
                            "timeAndMaterials": null,
                            "keyValues": [],                                "historicalKeyValues": []
                        },
                        "childTasks": get_tasks_param(data, x['PROJ_ID'], root_dept_uri, x['LVL_NO'])
                    }
        
        def get_task_params():
            task_data = rail.result('get_task_info_from_replicon')
            task_hierarchy = []
            root_project_id = rail.get_dag_run_conf()[
                'item']['root_project_id']
            code_parent_mapping = {}
            data = list(
                map(lambda x: x['row']['data'], get_costpoint_projects_data()))
            # In WBS boundary / chargeable-only mode the costpoint export does not
            # start at LVL_NO 1 (the root project sits at its own deeper level), so
            # the prime-level filter must start from the root project's level instead
            # of the hardcoded 0 used for full exports. Otherwise no rows match and
            # taskHierarchy comes back empty.
            start_level = 0
            if (bool(getattr(config, 'enable_wbs_boundary_sync', False))
                    or bool(getattr(config, 'allow_only_chargeable', False))):
                root_project_info = next(
                    filter(lambda x: x['PROJ_ID'] == root_project_id, data), None)
                start_level = (root_project_info['LVL_NO'] - 1) if root_project_info else 0
            get_apply_tasks_param(
                task_hierarchy, root_project_id, start_level, None, data, code_parent_mapping, None, len(data), '')
            
            if config.multi_plc_subtask_mode: 
                #UPDATE SCENARIO
                plc_mapping = rail.result('get_billing_rates_costpoint')
                plc_tasks = []
                for task in task_hierarchy:
                    added_role = []
                    parent_task_code = task['taskModificationToApply']['codeToApply']['value']
                    parent_task_name = task['taskModificationToApply']['name']
                    plc_assignments =  list(filter( lambda x: x['projectid'] == parent_task_code, plc_mapping))
                    parent_hierarchy = get_parent_hierarchy(parent_task_code)
                    parent_task_uri = get_task_uri(task_data, '', parent_hierarchy)
                    if plc_assignments: 
                        for assignment in plc_assignments:
                            if assignment['role'] is not None and assignment['role'] not in added_role:
                                plc_task_code = get_plc_task_code(parent_task_code)
                                plc_task_name = get_plc_task_name(assignment['role'], parent_task_name)
                                plc_task_uri = get_task_uri(task_data, plc_task_code, parent_hierarchy, assignment['role'])
                                costpoint_active_status_label = next(
                                    (cf['dropDownOption']['name']
                                     for cf in (task['taskModificationToApply'].get('customFieldsToApply') or [])
                                     if (cf.get('dropDownOption') or {}).get('name') in ('Active', 'Inactive')),
                                    'Active'
                                )
                                plc_task = get_plc_task_update(plc_task_uri, parent_task_uri, parent_task_name, parent_task_code, root_project_id, plc_task_name, \
                                    plc_task_code, task['taskModificationToApply']['timeEntryStartDateToApply']['date'], \
                                    task['taskModificationToApply']['timeEntryEndDateToApply']['date'], \
                                    task['taskModificationToApply']['isTimeEntryAllowed'], code_parent_mapping, costpoint_active_status_label,
                                    parent_proj_id=parent_task_code)
                                
                                added_role.append(assignment['role'])
                                plc_tasks.append(plc_task)
                        task['taskModificationToApply']['isTimeEntryAllowed'] = False

                if len(plc_tasks) :
                    task_hierarchy.extend(plc_tasks)
            return {
                "project": {
                    "code": root_project_id,
                },
                "taskHierarchy": task_hierarchy,
                "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }
        
        def get_parent_hierarchy(parent_task_code):
            root_project_id = rail.get_dag_run_conf()[
                'item']['root_project_id']
            if parent_task_code == root_project_id:
                return root_project_id
            # Strip the root prefix with real prefix removal (str.lstrip is
            # character-set based and over-strips). The root code may itself
            # contain '.' separators (e.g. WBS-boundary sync where the root is a
            # sub-WBS), so it must be treated as a single, non-split first
            # segment; descendant levels are then appended as full dotted codes
            # to match the taskCodeHierarchy keys.
            if parent_task_code.startswith(root_project_id + '.'):
                relative = parent_task_code[len(root_project_id) + 1:]
                task_hierarchy = root_project_id
                current_code = root_project_id
            else:
                # Defensive fallback: code not under the root.
                relative = parent_task_code
                task_hierarchy = ''
                current_code = ''
            for code in relative.split('.'):
                current_code = (current_code + '.' + code) if current_code else code
                task_hierarchy = (task_hierarchy + '/' + current_code) if task_hierarchy else current_code
            return task_hierarchy

        
        def get_plc_task_update(target_task_uri, parent_task_uri, parent_task_name, parent_task_code, root_project_id, plc_task_name, plc_task_code, task_start_date, task_end_date, isTimeEntryAllowed, code_parent_mapping, costpoint_active_status_label='Active', parent_proj_id=None):
            active_status_uri = _active_status_uri()
            parent = code_parent_mapping[parent_task_code] if parent_task_code in code_parent_mapping else None
            parent_request = {
                    "uri": parent_task_uri,
                    "name": None if parent_task_uri else parent_task_name,
                    "parent": parent,
                    "project": {
                        "code": root_project_id,
                    } if parent_task_uri is None and plc_task_code == root_project_id else None,
                    "parameterCorrelationId": null
                }
            return {
                    "target": {
                        "uri": target_task_uri,
                        "parent": parent_request if target_task_uri is None else None,
                        "project": {
                            "code": root_project_id,
                        } if target_task_uri is None else None,
                    },
                    "taskModificationToApply": {
                        "name": plc_task_name,
                        "codeToApply": {
                            "value": plc_task_code
                        },
                        "descriptionToApply": {
                            "value": plc_task_name + " - " + plc_task_code
                        },
                        "isClosed": _closed_flag(parent_proj_id or parent_task_code),
                        "timeEntryStartDateToApply": {
                            "date": task_start_date
                        },
                        "timeEntryEndDateToApply": {
                            "date": task_end_date
                        },
                        "timeAndExpenseEntryTypeToApply": {
                            "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
                        },
                        "isTimeEntryAllowed": isTimeEntryAllowed,
                        "customFieldsToApply": [
                            {
                                "customField": {"uri": active_status_uri},
                                "text": null,
                                "date": null,
                                "dropDownOption": {
                                    "uri": null,
                                    "name": costpoint_active_status_label
                                },
                                "number": null
                            }
                        ] if active_status_uri else [],
                    }
                }

        def get_task_uri(data, task_code, parent_hierarchy, plc_code = ''):
            hierarchy = parent_hierarchy
            if task_code:
                hierarchy = (((parent_hierarchy + '/' ) if parent_hierarchy else '' ) + task_code) if parent_hierarchy else task_code
            
            if plc_code:
                #multiple plc tasks will have same hierarchy and code .. so check for name 
                filtered_tasks = list(filter(lambda p: p['taskCodeHierarchy'] == hierarchy and p['name'].startswith(plc_code + ' - ') , data))
                if filtered_tasks and len(filtered_tasks) == 1:
                    return filtered_tasks[0]['uri']
                else:
                    #task with task code not found.. find with plc code so that GH data can be corrected
                    hierarchy = (((parent_hierarchy + '/' ) if parent_hierarchy else '' ) + plc_code + ' - ' + task_code) if parent_hierarchy else plc_code + ' - ' + task_code
                    task_uri =  rail.find_first_by_attr_and_get_attr(data, 'taskCodeHierarchy', hierarchy, 'uri', None)
                    return task_uri
            else :
                #nothing matching with name and code 
                task_uri =  rail.find_first_by_attr_and_get_attr(data, 'taskCodeHierarchy', hierarchy, 'uri', None)
                if task_uri: 
                    return task_uri
            return None
        
        def get_descendent_task_uri(data, task_code, parent_hierarchy, plc_code = ''):
            hierarchy = parent_hierarchy
            if task_code:
                hierarchy = (((parent_hierarchy + '/' ) if parent_hierarchy else '' ) + task_code) if parent_hierarchy else task_code
            
            if plc_code:
                #multiple plc tasks will have same hierarchy and code .. so check for name 
                filtered_tasks = list(filter(lambda p: p['taskCodeHierarchy'] == hierarchy and p['taskName'].startswith(plc_code + ' - ') , data))
                if filtered_tasks and len(filtered_tasks) == 1:
                    return filtered_tasks[0]['taskUri']
                else:
                    #task with task code not found.. find with plc code so that GH data can be corrected
                    hierarchy = (((parent_hierarchy + '/' ) if parent_hierarchy else '' ) + plc_code + ' - ' + task_code) if parent_hierarchy else plc_code + ' - ' + task_code
                    task_uri =  rail.find_first_by_attr_and_get_attr(data, 'taskCodeHierarchy', hierarchy, 'taskUri', None)
                    return task_uri
            else :
                #nothing matching with name and code 
                task_uri =  rail.find_first_by_attr_and_get_attr(data, 'taskCodeHierarchy', hierarchy, 'taskUri', None)
                if task_uri: 
                    return task_uri
            return None


        def get_apply_tasks_param(task_hierarchy, root_project_id, level_no, parent_req, data, code_parent_mapping, parent_task_code = None, num_records = 1, parent_hierarchy ='', parent_task_name = '', retained_ids = None):
            active_status_uri = _active_status_uri()
            # Compute the retained-PROJ_ID set once (top-level call) and thread it through
            # the recursion so leaf-level exclusion + ancestor retention is applied.
            if retained_ids is None:
                retained_ids = _cached_retained_proj_ids()
            prime_level = filter(lambda x: x['LVL_NO'] == level_no + 1
                                 and x['PROJ_ID'].startswith(root_project_id if parent_task_code is None else parent_task_code), data)
            task_data = rail.result('get_task_info_from_replicon')
            
            for prime_level_info in prime_level:
                task_code = prime_level_info['PROJ_ID']
                target_task_uri = get_task_uri(task_data, task_code, parent_hierarchy)
                is_excluded = task_code not in retained_ids
                if is_excluded and not target_task_uri:
                    continue
                parent_request = None
                attach_to_root_level = (
                        config.wbs_sync_boundary_level + 1
                        if bool(getattr(config, 'enable_wbs_boundary_sync', False))
                        else 2)
                if target_task_uri:
                    parent_request = None
                elif parent_task_code in code_parent_mapping:
                    parent_request_parent = code_parent_mapping[parent_task_code]
                    parent_task_uri = get_task_uri(task_data, '', parent_hierarchy) 
                    parent_request = {
                            "uri": parent_task_uri,
                            "name": None if parent_task_uri else parent_task_name,
                            "parent": parent_request_parent,
                            "project": {
                                "code": root_project_id,
                                } if parent_task_uri is None and attach_to_root_level > level_no else None,
                            "parameterCorrelationId": None
                            }
                else:
                    attach_to_root_level = (
                        config.wbs_sync_boundary_level + 1
                        if bool(getattr(config, 'enable_wbs_boundary_sync', False))
                        else 2)
                    if num_records == 1 and (bool(getattr(config, 'enable_wbs_boundary_sync', False))
                            or bool(getattr(config, 'allow_only_chargeable', False))):
                        #use parent req from the method call
                        parent_request = parent_req
                    elif prime_level_info['LVL_NO'] == attach_to_root_level:
                        project_task_name = ''
                        root_task_uri = get_task_uri(task_data, root_project_id, '')
                        if not root_task_uri:
                            project_task_name = rail.find_first_by_attr_and_get_attr(data, 'PROJ_ID', root_project_id,
                                                                                     'PROJ_NAME', None)
                        parent_request = {
                            "uri": root_task_uri,
                            "name": None if root_task_uri else project_task_name,
                            "parent": None,
                            "project": {
                                "code": root_project_id,
                                } if root_task_uri is None else None,
                                "parameterCorrelationId": None
                            }
                        parent_req = parent_request
                    else :
                        #use parent req from the method call
                        parent_request = parent_req

                code_parent_mapping[task_code] = parent_request

                task_name = prime_level_info['PROJ_NAME'] if len(list(filter(lambda p: p['PROJ_NAME'] == prime_level_info['PROJ_NAME'], data))) == 1 else f"{prime_level_info['PROJ_NAME']}_{prime_level_info['PROJ_ID']}"

                task_hierarchy.append({
                    "target": {
                        "uri": target_task_uri,
                        "parent": parent_request,
                        "project": {
                            "code": root_project_id,
                        } if target_task_uri is None else None,
                    },
                    "taskModificationToApply": {
                        "name": task_name,
                        "codeToApply": {
                            "value": prime_level_info['PROJ_ID']
                        },
                        "descriptionToApply": {
                            "value": prime_level_info['PROJ_LONG_NAME']
                        },
                        "isClosed": "true" if is_excluded else _closed_flag(prime_level_info['PROJ_ID']),
                        "timeEntryStartDateToApply": {
                            "date": rail.parse_date(prime_level_info.get('PROJ_START_DT'), config.date_time_format)
                        },
                        "timeEntryEndDateToApply": {
                            "date": rail.parse_date(prime_level_info.get('PROJ_END_DT'), config.date_time_format)
                        },
                        "timeAndExpenseEntryTypeToApply": {
                            "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
                        },
                        "isTimeEntryAllowed": "false" if is_excluded else ("true" if prime_level_info['ALLOW_CHARGES_FL'] == 'Y' and prime_level_info['ACTIVE_FL'] == 'Y' and prime_level_info.get('TC_PROJ_FL') not in ('E', 'N') else "false"),
                        "customFieldsToApply": [
                            {
                                "customField": {"uri": active_status_uri},
                                "text": null,
                                "date": null,
                                "dropDownOption": {
                                    "uri": null,
                                    "name": "Inactive" if is_excluded else ("Active" if _is_eligible(prime_level_info) else "Inactive")
                                },
                                "number": null
                            }
                        ] if active_status_uri else [],
                    }
                })

                next_parent_req = {
                    "uri": target_task_uri,
                    "name": None if target_task_uri else task_name,
                    "parent": None if target_task_uri else parent_req,
                    "project": {
                        "code": root_project_id,
                    } if target_task_uri is None else None,
                    "parameterCorrelationId": None
                }

                parent_hierarchy_code = (parent_hierarchy + '/' + task_code) if parent_hierarchy else task_code

                get_apply_tasks_param(
                    task_hierarchy, root_project_id, prime_level_info['LVL_NO'], next_parent_req, data, code_parent_mapping, task_code, num_records, parent_hierarchy_code, task_name, retained_ids)

        def _resolved_pm_emp_id_or_none():
            """Mode 2 (is_project_role_assigment_enabled True): returns the PM employeeId from Project Roles.
            Mode 1: returns None."""
            if bool(getattr(config, 'is_project_role_assigment_enabled', False)):
                return (rail.result('resolve_pm_comanagers') or {}).get('pm_emp_id')
            return None

        def _include_project_leader():
            """Mode 2: include only when Project Roles produced a PM.
            Mode 1: include when EMPL_ID-based Replicon user lookup succeeded."""
            if bool(getattr(config, 'is_project_role_assigment_enabled', False)):
                return bool(_resolved_pm_emp_id_or_none())
            return bool(rail.result('get_project_leader_info_from_replicon'))

        def _project_leader_emp_id(root_project_info):
            """Mode 2: PM employeeId from Project Roles.
            Mode 1: EMPL_ID from the root project info."""
            if bool(getattr(config, 'is_project_role_assigment_enabled', False)):
                return _resolved_pm_emp_id_or_none()
            return root_project_info.get('EMPL_ID')

        def get_add_project_and_task_param():
            root_dept_uri = f"urn:replicon-tenant:{rail.get_tenant_slug()}:department:1"
            root_project_id, data, root_project_info = get_project_data()
            is_root_active = _root_active(root_project_info)
            # Tasks are no longer created inline with the project; they are synced
            # via update_task (CreateTaskHierarchyOrApplyModifications) after the
            # project is added, so "tasks" is sent empty below.
            # root_task = get_root_task(root_project_info, root_dept_uri)
            # child_tasks = get_tasks_param(
            #                     data, root_project_id, root_dept_uri,
            #                     root_project_info['LVL_NO'] if (bool(getattr(config, 'enable_wbs_boundary_sync', False))
            #                                                     or bool(getattr(config, 'allow_only_chargeable', False))) else 1, root_task)
            # if('childTasks' in root_task):
            #     root_task['childTasks'].extend(child_tasks)
            # else:
            #     root_task["childTasks"] = child_tasks
            return {
                "project": {
                    "target": {
                        "uri": null,
                        "name": null,
                        "code": root_project_info['PROJ_ID'],
                        "parameterCorrelationId": null
                    },
                    "projectInfo": {
                        "name": root_project_info['PROJ_NAME'],
                        "code":  root_project_info['PROJ_ID'],
                        "description":  root_project_info['PROJ_LONG_NAME'],
                        "timeEntryDateRange": {
                            "startDate": rail.parse_date(root_project_info.get('PROJ_START_DT'), config.date_time_format),
                            "endDate": rail.parse_date(root_project_info.get('PROJ_END_DT'), config.date_time_format),
                        },
                        # "projectStatusLabel": {
                        #     "uri": null,
                        #     "name": 'In-Progress' if root_project_info['ACTIVE_FL'] == 'Y' else 'Cancelled'
                        # },
                        "percentCompleted": "0",
                        "clients": [
                            {
                                "client": {
                                    "uri": null,
                                    "name": root_project_info['CUST_NAME'],
                                    "code": null,
                                    "parameterCorrelationId": null
                                },
                                "costAllocationPercentage": 100
                            }
                        ] if root_project_info.get('CUST_NAME') else [],
                        "program": null,
                        "projectLeader": {
                            "uri": null,
                            "loginName": null,
                            "employeeId": _project_leader_emp_id(root_project_info),
                            "parameterCorrelationId": null
                        } if _include_project_leader() else null,
                        "customFieldValues": [
                            {
                                "customField": {
                                    "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Purchase Order No', 'uri'),
                                },
                                "text": root_project_info.get('CUST_PO_ID')
                            },
                            {
                                "customField": {
                                    "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Project Classification', 'uri'),
                                },
                                "text": root_project_info.get('S_PROJ_RPT_DC')
                            },
                            {
                                "customField": {
                                    "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Company', 'uri'),
                                },
                                "text": rail.get_dag_run_conf()['item']['_company']
                                if (bool(getattr(config, 'allow_only_chargeable', False))
                                    or bool(getattr(config, 'enable_wbs_boundary_sync', False)))
                                else rail.get_dag_run_conf()['item']['data'][0].get('_company')
                            },
                            {
                                "customField": {
                                    "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Opportunity ID', 'uri'),
                                },
                                "text": root_project_info.get('OPP_ID')
                            },
                            {
                                "customField": {
                                    "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Source System', 'uri'),
                                },
                                "text": "Costpoint"
                            },
                            *([{
                                "customField": {
                                    "uri": _active_status_uri('project_udfs'),
                                },
                                "text": null,
                                "date": null,
                                "dropDownOption": {
                                    "uri": null,
                                    "name": "Active" if is_root_active else "Inactive"
                                },
                                "number": null
                            }] if _active_status_uri('project_udfs') else []),
                        ],
                        "isTimeEntryAllowed": "false",
                        "costTypeUri": null,
                        "estimatedHours": null,
                        "estimatedCost": null,
                        "estimatedExpenses": null,
                        "budget": null,
                        "isProjectLeaderApprovalRequired": "true",
                        "estimationModeUri": "urn:replicon:project-estimation-mode:task-based",
                        "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                        "timeAndMaterials": {
                            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                            "billingRateFrequency": null,
                            "billingRateFrequencyDuration": null,
                            "billingRates": []
                        },
                        "defaultBillingCurrency": null
                    },
                    "tasks": [],
                    "team": {
                        "teamMembers": get_project_resource_param_project(root_dept_uri, root_project_info)
                    },
                    "expenses": null,
                    "timeAndMaterials": null,
                    "fixedBid": null
                }
            }
        
        def get_root_task(root_project_info, root_dept_uri):
            active_status_uri = _active_status_uri()
            return {
                "task": {
                    "target": {
                        "name": root_project_info['PROJ_NAME']
                    },
                    "name": root_project_info['PROJ_NAME'],
                    "code": root_project_info['PROJ_ID'],
                    "description": root_project_info['PROJ_LONG_NAME'],
                    "timeEntryDateRange": {
                        "startDate": rail.parse_date(root_project_info.get('PROJ_START_DT'), config.date_time_format),
                        "endDate": rail.parse_date(root_project_info.get('PROJ_END_DT'), config.date_time_format),
                    },
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "true" if root_project_info['ALLOW_CHARGES_FL'] == 'Y' and root_project_info['ACTIVE_FL'] == 'Y' and root_project_info.get('TC_PROJ_FL') not in ('E', 'N') else "false",
                    "estimatedHours": null,
                    "isClosed": _closed_flag(root_project_info['PROJ_ID']),
                    "customFieldValues": [
                        {
                            "customField": {"uri": active_status_uri},
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": null,
                                "name": "Active" if _is_eligible(root_project_info) else "Inactive"
                            },
                            "number": null
                        }
                    ] if active_status_uri else [],
                    "extensionFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "assignedResources": get_assigned_resources(root_dept_uri, root_project_info['PROJ_WORK_FRC_FL'], root_project_info['PROJ_ID']),
                    "timeAndMaterials": null,
                    "keyValues": [],
                    "historicalKeyValues": []
                }
            }

        def get_project_data():
            root_project_id = rail.get_dag_run_conf()[
                'item']['root_project_id']
            data = list(
                map(lambda x: x['row']['data'], get_costpoint_projects_data()))
            root_project_info = next(filter(
                lambda x: x['PROJ_ID'] == root_project_id, data), None)
            return root_project_id, data, root_project_info

        def do_get_task_info_from_replicon():
            tasks = []
            task_tree = _build_task_tree_from_task_list(
                rail.result('get_task_list_info'))
            if task_tree:
                cp_data = get_project_data()[1]
                get_task_data_replicon(task_tree, tasks, cp_data, '')
            return tasks

        def get_new_task_name(data, task_code, task_name, task_hierarchy):
            hierarchy_codes = task_hierarchy.split('/')
            #do not apply new name for plc_task
            if len(hierarchy_codes)>1:
                if(hierarchy_codes[-1] == hierarchy_codes[-2]):
                    return task_name
            filtered_task = list(
                filter(lambda x: x['PROJ_ID'] == task_code, data))
            task_info = filtered_task[0] if filtered_task else null
            if task_info:
                tasks_by_name = list(
                    filter(lambda x: x['PROJ_NAME'] == task_info['PROJ_NAME'], data))
                return task_info['PROJ_NAME'] if len(tasks_by_name) == 1 else f"{task_info['PROJ_NAME']}_{task_info['PROJ_ID']}"
            else:
                return task_name
        
        def get_task_data_replicon(tasks, result, cp_data, parent_hierarchy):
            for task in tasks:
                if task['task']['code']:
                    task_hierarchy = ((parent_hierarchy + "/") if parent_hierarchy else '') + task['task']['code']
                    result.append(
                        {'code': task['task']['code'], 'name': task['task']['name'], 'uri': task['task']['uri'], 'new_name': get_new_task_name(cp_data, task['task']['code'], task['task']['name'], task_hierarchy), 'taskCodeHierarchy': task_hierarchy})
                get_task_data_replicon(task['childTasks'], result, cp_data, task_hierarchy)

        get_task_list_info = rail.RepliconServiceOperator(
            task_id='get_task_list_info',
            endpoint='/services/TaskListService1.svc/GetData',
            data=lambda: {
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:task-list-column:task",
                    "urn:replicon:task-list-column:code",
                    "urn:replicon:task-list-column:name",
                    "urn:replicon:task-list-column:parent"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": None,
                        "operatorUri": None,
                        "rightExpression": None,
                        "value": None,
                        "filterDefinitionUri": "urn:replicon:task-list-filter:project"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": None,
                        "operatorUri": None,
                        "rightExpression": None,
                        "value": {
                            "uri": (rail.result('get_project_basic_details') or {}).get('uri'),
                            "uris": [],
                            "bool": None,
                            "date": None,
                            "money": None,
                            "number": None,
                            "text": None,
                            "time": None,
                            "calendarDayDurationValue": None,
                            "workdayDurationValue": None,
                            "dateRange": None,
                            "dateTimeUtc": None,
                            "dateTimeUtcRange": None,
                            "numberRange": None
                        },
                        "filterDefinitionUri": None
                    },
                    "value": None,
                    "filterDefinitionUri": None
                }
            }
        )

        get_task_info_from_replicon = rail.PythonOperator(
            task_id='get_task_info_from_replicon',
            python_callable=do_get_task_info_from_replicon
        )

        rename_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='rename_tasks',
            items=lambda: list(filter(lambda x: x['name'] != x['new_name'], rail.result(
                'get_task_info_from_replicon'))),
            endpoint="/services/TaskService1.svc/UpdateName",
            data=lambda item: {
                "taskUri": item['uri'],
                "name": item['new_name']
            }
        )

        if_project_present = rail.IfOperator(
            task_id='if_project_present',
            test='''{{ result('get_project_basic_details') | is_truthy }}''',
            yes_task="update_project",
            no_task="check_should_create",
        )

        check_should_create = rail.IfOperator(
            task_id='check_should_create',
            test=lambda: (
                (not _is_type_or_classification_excluded(get_project_data()[2] or {})
                 and bool(_cached_retained_proj_ids()))
                if _has_exclusions()
                else (
                    (get_project_data()[2] or {}).get('ACTIVE_FL') == 'Y'
                    and (get_project_data()[2] or {}).get('TC_PROJ_FL') not in config.excluded_project_type_flags
                )
                if not _require_chargeable_leaf()
                else any(_is_eligible(row) for row in (get_project_data()[1] or []))
            ),
            yes_task='add_project_and_task',
            no_task='finish',
        )

        add_project_and_task = rail.RepliconServiceOperator(
            task_id='add_project_and_task',
            endpoint="/services/ImportService1.svc/PutProject4",
            data=get_add_project_and_task_param
        )

        def update_project_create_or_modifiy():
            root_project_id = rail.get_dag_run_conf()[
                'item']['root_project_id']
            data = list(
                map(lambda x: x['row']['data'], get_costpoint_projects_data()))
            root_project_info = next(filter(
                lambda x: x['PROJ_ID'] == root_project_id, data), None)

            division_name = rail.find_first_by_attr_and_get_attr(
                rail.get_dag_run_conf()['divisions'], 'code', get_project_data()[2]['ORG_ID'], 'uri')

            # Decide project status:
            # - root closable (self + every descendant ACTIVE_FL='N') -> Archived
            # - exclusion lists configured AND no qualifying leaf survives anywhere
            #   under the root (_root_active() False) -> Archived; the root returns
            #   to In Progress as soon as a leaf moves back into inclusion
            # - otherwise -> In Progress
            root_closable = bool(root_project_info) and (root_project_info['PROJ_ID'] in _closable_ids())
            is_root_active = _root_active(root_project_info)
            _has_excl = _has_exclusions()
            project_status_name = 'Archived' if (root_closable or (_has_excl and not is_root_active)) else 'In Progress'

            return {
                "target": {
                    "code": root_project_info['PROJ_ID'],
                },
                "modifications": {
                    "nameToApply": {
                        "value": root_project_info['PROJ_NAME']
                    },
                    "codeToApply": {
                        "value": root_project_info['PROJ_ID']
                    },
                    "descriptionToApply": {
                        "value": root_project_info['PROJ_LONG_NAME']
                    },
                    "percentCompletedToApply": "0",
                    "startDateToApply": {
                        "date": rail.parse_date(root_project_info.get('PROJ_START_DT'), config.date_time_format)
                    },
                    "endDateToApply": {
                        "date": rail.parse_date(root_project_info.get('PROJ_END_DT'), config.date_time_format)
                    },
                    "billingTypeToApply": {
                        "value": "urn:replicon:billing-type:time-and-material"
                    },
                    "clientAssignmentsSchedulesToApply": {
                        "clients": [
                            {
                                "client": {
                                    "name": root_project_info['CUST_NAME'],
                                },
                                "costAllocationPercentage": 100
                            }
                        ],
                        "effectiveDate": null
                    } if root_project_info.get('CUST_NAME') else null,
                    "projectLeaderToApply": {
                        "user": {
                            "employeeId": _project_leader_emp_id(root_project_info),
                        }
                    } if _include_project_leader() else null,
                    "isProjectLeaderApprovalRequired": config.project_leader_approval,
                    "isTimeEntryAllowed": "true" if (root_project_info['ALLOW_CHARGES_FL'] == 'Y' and root_project_info['ACTIVE_FL'] == 'Y' and root_project_info.get('TC_PROJ_FL') not in ('E', 'N') and (not _has_excl or is_root_active)) else "false",
                    "timeAndMaterials": {
                        "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                    },
                    "fixedBid": null,
                    "customFieldsToApply": [
                        {
                            "customField": {
                                "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Purchase Order No', 'uri'),
                            },
                            "text": root_project_info.get('CUST_PO_ID')
                        },
                        {
                            "customField": {
                                "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Project Classification', 'uri'),
                            },
                            "text": root_project_info.get('S_PROJ_RPT_DC')
                        },
                        {
                            "customField": {
                                "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Company', 'uri'),
                            },
                            "text": rail.get_dag_run_conf()['item']['_company']
                            if (bool(getattr(config, 'allow_only_chargeable', False))
                                or bool(getattr(config, 'enable_wbs_boundary_sync', False)))
                            else rail.get_dag_run_conf()['item']['data'][0].get('_company')
                        },
                        {
                            "customField": {
                                "uri":   rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['project_udfs'], 'textValue', 'Opportunity ID', 'uri'),
                            },
                            "text": root_project_info.get('OPP_ID')
                        },
                        *([{
                            "customField": {
                                "uri": _active_status_uri('project_udfs'),
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": null,
                                "name": "Active" if is_root_active else "Inactive"
                            },
                            "number": null
                        }] if _active_status_uri('project_udfs') else []),
                    ],
                    "divisionToApply": {
                        "division": {"uri": division_name}
                    } if division_name else None,
                    "keyValuesToApply": get_polaris_key_values(),
                    "statusToApply": {"name": project_status_name},
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }

        def get_polaris_key_values():
            keyValuesToApply = [
                {
                    "keyUri": 'polaris-psa:slack-channel',
                    "value": {}
                },
                {
                    "keyUri": 'urn:replicon:project-key-value-key:external-dependency',
                    "value": {
                        "collection": [
                            {
                                "text": 'Project has resource requests',
                                "uri": 'urn:replicon:external-dependency:psa'
                            }
                        ]
                    }
                },
                {
                    "keyUri": 'urn:replicon:project-key-value-key:project-management-type',
                    "value": {
                        "uri": 'urn:replicon:project-management-type:managed'
                    }
                }
            ]

            return keyValuesToApply

        update_project = rail.RepliconServiceOperator(
            task_id='update_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=update_project_create_or_modifiy
        )

        def get_update_task_data(items):
            return {
                "project": {
                    "code": rail.get_dag_run_conf()['item']['root_project_id'],
                },
                "taskHierarchy": list(items),
                "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }

        update_task = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_task',
            items=lambda: get_task_params()['taskHierarchy'],
            batch_size=int(getattr(config, 'task_hierarchy_batch_size', 50)),
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            data=get_update_task_data
        )

        def update_managed_project():
            return {
                "target": {
                    "uri": rail.result('add_project_and_task')['uri']
                },
                "modifications": {
                    "keyValuesToApply": get_polaris_key_values(),
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }

        update_manage_project = rail.RepliconServiceOperator(
            task_id='update_manage_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=update_managed_project
        )

        def get_project_resource_param_project(root_dept_uri, root_project_info):
            if root_project_info['PROJ_WORK_FRC_FL'] != 'Y':
                return [
                    {
                        "resource": {
                            "uri": root_dept_uri
                        },
                        "timeAndMaterials": {
                            "billingRatesAllowedForBillingTimeUris": [
                                "urn:replicon:project-specific-billing-rate",
                            ]
                        }
                    }
                ]
            return list(map(lambda x: {
                        "resource": {
                            "uri": null,
                            "resourcePlaceholderParameterCorrelationId": null,
                            "user":
                                {"uri": rail.find_first_by_attr_and_get_attr(rail.result(
                                    'get_users_from_replicon'), 'employeeId', x, 'userDetails')['uri']}
                        },
                        "timeAndMaterials": null
                        }, filter(lambda x: x and rail.find_first_by_attr_and_get_attr(rail.result('get_users_from_replicon'), 'employeeId', x, 'userDetails'),
                                  map_workforce_empid())))

        def get_project_resource_billingrates(empid):
            data = []
            for rate in filter(lambda x: x['employeeId'] == empid, rail.result('get_billing_rates_costpoint')):
                data.append(rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()[
                            'billing_rates'], 'code', rate['role'], 'uri'))
            return data

        update_division = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_division',
            items=lambda: [1] if get_project_data()[2] and rail.find_first_by_attr_and_get_attr(
                rail.get_dag_run_conf()['divisions'], 'code', get_project_data()[2]['ORG_ID']) else [],
            endpoint="/services/ProjectService1.svc/UpdateDivision2",
            data=lambda: {"projectUri": (rail.result('add_project_and_task') or {}).get('uri') or (rail.result('get_project_basic_details') or {}).get('uri'),
                          "division": {"uri": rail.find_first_by_attr_and_get_attr(
                              rail.get_dag_run_conf()['divisions'], 'code', get_project_data()[2]['ORG_ID'], 'uri')}}
        )

        add_log_entry = rail.WriteLogOperator(
            task_id='add_log_entry',
            log="{{ result('create_log') }}",
            message="na",
            severity="Success",
            items=lambda: rail.get_dag_run_conf()['item']['data'],
            properties={
                "proj_id": "{{ item.row.data.PROJ_ID }}",
                "proj_name":  "{{ item.row.data.PROJ_NAME }}",
                "action": "{{ 'Add'  if result('get_project_basic_details') | is_falsy else 'Update' }}",
                "status": "Success",
                "details": "",
            }
        )

        project_setup_complete = rail.EmptyOperator(
            task_id='project_setup_complete',
            trigger_rule='none_failed_min_one_success',
        )

        has_co_managers_to_assign = rail.IfOperator(
            task_id='has_co_managers_to_assign',
            test=lambda: bool((rail.result('resolve_pm_comanagers') or {}).get('co_manager_user_uris')),
            yes_task='assign_comanager_to_project',
            no_task='get_project_task_list',
        )

        assign_comanager_to_project = rail.RepliconServiceOperator(
            task_id="assign_comanager_to_project",
            endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
            data=lambda: {
                "projectUri": (
                    (rail.result('get_project_basic_details') or {}).get('uri')
                    or (rail.result('add_project_and_task') or {}).get('uri')
                ),
                "sharedUris": (rail.result('resolve_pm_comanagers') or {}).get('co_manager_user_uris') or [],
            }
        )

        get_project_task_list = rail.RepliconServiceOperator(
            task_id='get_project_task_list',
            endpoint='/services/TaskListService1.svc/GetData',
            data=lambda: {
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:task-list-column:task",
                    "urn:replicon:task-list-column:code",
                    "urn:replicon:task-list-column:name",
                    "urn:replicon:task-list-column:parent"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": None,
                        "operatorUri": None,
                        "rightExpression": None,
                        "value": None,
                        "filterDefinitionUri": "urn:replicon:task-list-filter:project"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": None,
                        "operatorUri": None,
                        "rightExpression": None,
                        "value": {
                            "uri": (
                                (rail.result('get_project_basic_details') or {}).get('uri')
                                or (rail.result('add_project_and_task') or {}).get('uri')
                            ),
                            "uris": [],
                            "bool": None,
                            "date": None,
                            "money": None,
                            "number": None,
                            "text": None,
                            "time": None,
                            "calendarDayDurationValue": None,
                            "workdayDurationValue": None,
                            "dateRange": None,
                            "dateTimeUtc": None,
                            "dateTimeUtcRange": None,
                            "numberRange": None
                        },
                        "filterDefinitionUri": None
                    },
                    "value": None,
                    "filterDefinitionUri": None
                }
            },
            data_handler=lambda data: _flatten_task_list_response(
                data,
                {
                    (row.get('row') or {}).get('data', {}).get('PROJ_ID'):
                        (row.get('row') or {}).get(
                            'data', {}).get('PROJ_WORK_FRC_FL')
                    for row in (get_costpoint_projects_data() or [])
                    if (row.get('row') or {}).get('data', {}).get('PROJ_ID')
                }
            )
        )

        get_existing_task_resource_estimates = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_existing_task_resource_estimates',
            endpoint='/services/TaskService1.svc/GetPageOfTaskResourceEstimateSummaryForTasks',
            items=lambda: [
                t['taskUri']
                for t in (rail.result('get_project_task_list') or [])
                if t.get('taskUri')
            ],
            batch_size=5000,
            flatten=True,
            data=lambda items: {
                "page": "1",
                "pageSize": "1000000",
                "projectUri": (
                    (rail.result('get_project_basic_details') or {}).get('uri')
                    or (rail.result('add_project_and_task') or {}).get('uri')
                ),
                "taskUris": items,
                "filter": None
            }
        )

        def do_get_workforce_resources():
            billing_rates = rail.result('get_billing_rates_costpoint') or []
            tasks = rail.result('get_project_task_list') or []
            users_by_emp_id = {
                x['employeeId']: x['userDetails']
                for x in (rail.result('get_users_from_replicon') or [])
                if x and x.get('userDetails') and x.get('employeeId')
            }
            # Pre-index lookups (first occurrence wins, matching
            # find_first_by_attr_and_get_attr) so the per-group loops below do
            # O(1) lookups instead of repeated full scans of the conf billing
            # rates and the (potentially large) task list.
            billing_rate_uri_by_code = {}
            for r in (rail.get_dag_run_conf().get('billing_rates') or []):
                code = r.get('code')
                if code is not None and code not in billing_rate_uri_by_code:
                    billing_rate_uri_by_code[code] = r.get('uri')
            taskuri_by_hierarchy = {}
            tasks_by_code = {}
            for t in tasks:
                hierarchy = t.get('taskCodeHierarchy')
                if hierarchy is not None and hierarchy not in taskuri_by_hierarchy:
                    taskuri_by_hierarchy[hierarchy] = t.get('taskUri')
                tasks_by_code.setdefault(t.get('taskCode'), []).append(t)
            grouped = {}
            for rate in billing_rates:
                emp_id = rate.get('employeeId')
                project_id = rate.get('projectid')
                role_code = rate.get('role')
                if not emp_id or not project_id:
                    continue
                # Ensure the (emp_id, project_id) group exists even for
                # workforce users that have no PLC/billing role.
                roles = grouped.setdefault((emp_id, project_id), [])
                if role_code:
                    roles.append(role_code)
            result = []
            for (emp_id, project_id), role_codes in grouped.items():
                user_details = users_by_emp_id.get(emp_id)
                if not user_details:
                    continue
                role_uris = [
                    billing_rate_uri_by_code.get(role)
                    for role in role_codes
                ]
                parent_hierarchy = get_parent_hierarchy(project_id)
                taskUri = taskuri_by_hierarchy.get(parent_hierarchy)
                plc_estimate = {
                    'userUri': user_details['uri'],
                    'roles': [r for r in role_uris if r],
                    'taskUri': taskUri,
                }
                if config.multi_plc_subtask_mode and role_codes:
                    # Only tasks whose code == project_id are relevant (the
                    # original loop skipped all others).
                    for task in tasks_by_code.get(project_id, []):
                        task_code = task['taskCode']
                        taskCodeHierarchy = task['taskCodeHierarchy']

                        if not taskCodeHierarchy.endswith(task_code+'/'+task_code):
                            continue

                        task_name = task['taskName'] or ''

                        plc = task_name.split(' - ')[0].strip()
                        if plc not in role_codes:
                            continue
                        plc_uri = billing_rate_uri_by_code.get(plc)
                        if not plc_uri:
                            continue
                        result.append({
                                'userUri': user_details['uri'],
                                'roles': [plc_uri],
                                'taskUri': task['taskUri']
                            }
                        )
                else:
                    result.append(plc_estimate)
            return result

        get_workforce_resources = rail.PythonOperator(
            task_id='get_workforce_resources',
            python_callable=do_get_workforce_resources
        )

        def do_get_resources_delete():
            workforce_keys = set()
            for item in (rail.result('get_workforce_resources') or []):
                task_uri = item.get('taskUri')
                user_uri = item.get('userUri')
                if not task_uri or not user_uri:
                    continue
                roles = item.get('roles') or []
                if roles:
                    for role in roles:
                        workforce_keys.add((task_uri, user_uri, role))
                else:
                    # Role-less workforce user; key on role None so an existing
                    # role-less estimate for the same user is preserved.
                    workforce_keys.add((task_uri, user_uri, None))
            grouped = {}
            for x in (rail.result('get_existing_task_resource_estimates') or []):
                if not x:
                    continue
                task_uri = (x.get('task') or {}).get('uri')
                user_uri = (x.get('user') or {}).get('uri')
                role_uri = (x.get('projectRole') or {}).get('uri')
                estimate_uri = x.get('estimateUri')
                if not task_uri or not user_uri:
                    continue
                # Compare role-agnostically: an existing estimate (role-based or
                # role-less) that has no matching workforce entry is an extra
                # user to delete.
                if (task_uri, user_uri, role_uri) in workforce_keys:
                    continue
                if task_uri not in grouped:
                    grouped[task_uri] = {}
                if user_uri not in grouped[task_uri]:
                    grouped[task_uri][user_uri] = []
                grouped[task_uri][user_uri].append({
                    'roleuri': role_uri,
                    'estimateUri': estimate_uri
                })
            return [
                {
                    'taskUri': task_uri,
                    'users': [
                        {'userUri': user_uri, 'roleinfo': roleinfo}
                        for user_uri, roleinfo in users.items()
                    ]
                }
                for task_uri, users in grouped.items()
            ]

        get_resources_delete = rail.PythonOperator(
            task_id='get_resources_delete',
            python_callable=do_get_resources_delete
        )

        is_delete_resource = rail.IfOperator(
            task_id='is_delete_resource',
            test='''{{ (result('get_resources_delete') | is_truthy) and (result('get_project_basic_details') | is_truthy) }}''',
            yes_task='bulk_delete_task_resource_estimates',
            no_task='get_missing_task_resource_estimates',
        )

        def do_get_missing_task_resource_estimates():
            existing_estimate_map = {}
            existing_keys = set()
            existing_user_tasks = set()
            for x in (rail.result('get_existing_task_resource_estimates') or []):
                if not x:
                    continue
                task_uri = (x.get('task') or {}).get('uri')
                user_uri = (x.get('user') or {}).get('uri')
                role_uri = (x.get('projectRole') or {}).get('uri')
                if task_uri and user_uri:
                    # Track every existing (task, user) pair, including role-less
                    # assignments (projectRole == null), for idempotency.
                    existing_user_tasks.add((task_uri, user_uri))
                if task_uri and user_uri and role_uri:
                    key = (task_uri, user_uri, role_uri)
                    existing_keys.add(key)
                    existing_estimate_map[(task_uri, user_uri)] = x.get(
                        'estimateUri', '')
            grouped = {}
            for item in (rail.result('get_workforce_resources') or []):
                task_uri = item.get('taskUri')
                user_uri = item.get('userUri')
                if not task_uri or not user_uri:
                    continue
                roles = item.get('roles') or []
                missing_roleinfo = [
                    {'roleuri': role, 'estimateUri': existing_estimate_map.get(
                        (task_uri, user_uri), '')}
                    for role in roles
                    if (task_uri, user_uri, role) not in existing_keys
                ]
                # A role-based user whose roles all already exist has nothing
                # missing and is skipped. A role-less workforce user (roles == [])
                # still needs to be assigned to the project/task team, but only
                # when not already assigned to the task (idempotency); otherwise
                # the task keeps reporting the same user on every run.
                if roles and not missing_roleinfo:
                    continue
                if not roles and (task_uri, user_uri) in existing_user_tasks:
                    continue
                if task_uri not in grouped:
                    grouped[task_uri] = []
                grouped[task_uri].append({
                    'userUri': user_uri,
                    'roleinfo': missing_roleinfo
                })
            return [
                {'taskUri': task_uri, 'users': users}
                for task_uri, users in grouped.items()
            ]

        get_missing_task_resource_estimates = rail.PythonOperator(
            task_id='get_missing_task_resource_estimates',
            python_callable=do_get_missing_task_resource_estimates
        )

        has_missing_task_resource_estimates = rail.IfOperator(
            task_id='has_missing_task_resource_estimates',
            test='''{{ result('get_missing_task_resource_estimates') | is_truthy }}''',
            yes_task='assign_project_team_members',
            no_task='need_to_assign_all_users',
        )

        def has_non_workforce_descendant_tasks():
            return bool(getattr(config, 'assign_allusers_on_update', False)) and any(
                (t or {}).get('project_work_force_flag') == 'N'
                for t in (rail.result('get_project_task_list') or [])
            )

        need_to_assign_all_users = rail.IfOperator(
            task_id='need_to_assign_all_users',
            test=has_non_workforce_descendant_tasks,
            yes_task='assign_project_all_members',
            no_task='add_log_entry',
        )

        bulk_delete_project_team_members_assignment = rail.RepliconServiceOperator(
            task_id='bulk_delete_project_team_members_assignment',
            endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment2',
            data=lambda: {
                "projectUri": (
                    (rail.result('get_project_basic_details') or {}).get('uri')
                    or (rail.result('add_project_and_task') or {}).get('uri')
                ),
                "userUris": list({
                    u['userUri']
                    for t in (rail.result('get_resources_delete') or [])
                    for u in (t.get('users') or [])
                    if u.get('userUri')
                }),
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:unassign"
            }
        )

        bulk_delete_resource_assignments = rail.RepliconServiceCallForEachItemOperator(
            task_id='bulk_delete_resource_assignments',
            items=lambda: rail.result('get_resources_delete') or [],
            endpoint='/services/TaskService1.svc/BulkUpdateResourceAssignments',
            data=lambda item: {
                "taskUri": item.get('taskUri'),
                "resourceUris": [
                    u['userUri']
                    for u in (item.get('users') or [])
                    if u.get('userUri')
                ],
                "isAssigned": "0"
            }
        )

        bulk_delete_task_resource_estimates = rail.RepliconServiceCallForEachItemOperator(
            task_id='bulk_delete_task_resource_estimates',
            items=lambda: rail.result('get_resources_delete') or [],
            endpoint='/services/TaskService1.svc/BulkDeleteTaskResourceEstimates',
            data=lambda item: {
                "taskTarget": {
                    "uri": item.get('taskUri'),
                    "name": None,
                    "parent": None,
                    "project": None,
                    "parameterCorrelationId": None
                },
                "estimateUris": [
                    r['estimateUri']
                    for u in (item.get('users') or [])
                    for r in (u.get('roleinfo') or [])
                    if r.get('estimateUri')
                ]
            }
        )

        def get_project_resources(root_project_info, root_dept_uri):
            if root_project_info and root_project_info.get('PROJ_WORK_FRC_FL') != 'Y':
                user_uris = [root_dept_uri]
            
            if((root_project_info and root_project_info.get('PROJ_WORK_FRC_FL') == 'Y')) or bool(getattr(config, 'force_assign_user_resources', False)):
                user_uris = list({
                    u['userUri']
                    for t in (rail.result('get_missing_task_resource_estimates') or [])
                    for u in (t.get('users') or [])
                    if u.get('userUri')
                })
            return {
                "projectUri": (
                    (rail.result('get_project_basic_details') or {}).get('uri')
                    or (rail.result('add_project_and_task') or {}).get('uri')
                ),
                "resourceUri": user_uris,
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }

        assign_project_team_members = rail.RepliconServiceOperator(
            task_id='assign_project_team_members',
            endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            data=lambda: get_project_resources(
                get_project_data()[2],
                f"urn:replicon-tenant:{rail.get_tenant_slug()}:department:1"
            )
        )

        # Per-execution index: task_uri -> [missing-estimate entries]. Built once
        # so the per-task resource/estimate lookups below are O(1) instead of a
        # full scan of get_missing_task_resource_estimates for every task (which
        # is O(tasks x estimates) and was stalling assign_task_resources /
        # bulk_update_task_resource_estimates).
        _missing_estimates_by_task_memo = {}

        def _missing_estimates_by_task():
            if 'index' not in _missing_estimates_by_task_memo:
                index = {}
                for t in (rail.result('get_missing_task_resource_estimates') or []):
                    task_uri = t.get('taskUri')
                    if not task_uri:
                        continue
                    index.setdefault(task_uri, []).append(t)
                _missing_estimates_by_task_memo['index'] = index
            return _missing_estimates_by_task_memo['index']

        def get_resource_uris_for_task(task_uri, project_work_force_flag, root_dept_uri):
            if project_work_force_flag != 'Y':
                return [root_dept_uri]
            return [
                u['userUri']
                for t in _missing_estimates_by_task().get(task_uri, [])
                for u in (t.get('users') or [])
                if u.get('userUri')
            ]

        def get_task_resources(item):
            root_dept_uri = f"urn:replicon-tenant:{rail.get_tenant_slug()}:department:1"
            return {
                "taskUri": item['taskUri'],
                "resourceUris": get_resource_uris_for_task(item['taskUri'], item.get('project_work_force_flag'), root_dept_uri),
                "isAssigned": "1"
            }
            
        def get_tasks_with_resources():
            # Only call the service for tasks that have at least one resource URI
            # to assign; tasks with no resolved resourceUris are ignored.
            root_dept_uri = f"urn:replicon-tenant:{rail.get_tenant_slug()}:department:1"
            return [
                t for t in (rail.result('get_project_task_list') or [])
                if get_resource_uris_for_task(t['taskUri'], t.get('project_work_force_flag'), root_dept_uri)
            ]

        assign_task_resources = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_task_resources',
            items=get_tasks_with_resources,
            endpoint='/services/TaskService1.svc/BulkUpdateResourceAssignments',
            data=get_task_resources
        )

        def get_all_project_resources(root_dept_uri):
            return {
                "projectUri": (
                    (rail.result('get_project_basic_details') or {}).get('uri')
                    or (rail.result('add_project_and_task') or {}).get('uri')
                ),
                "resourceUri": [root_dept_uri],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }

        assign_project_all_members = rail.RepliconServiceOperator(
            task_id='assign_project_all_members',
            endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            data=lambda: get_all_project_resources(
                f"urn:replicon-tenant:{rail.get_tenant_slug()}:department:1"
            )
        )

        def get_all_task_resources(item):
            root_dept_uri = f"urn:replicon-tenant:{rail.get_tenant_slug()}:department:1"
            return {
                "taskUri": item['taskUri'],
                "resourceUris": [root_dept_uri],
                "isAssigned": "1"
            }

        assign_task_all_resources = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_task_all_resources',
            items=lambda: [
                t for t in (rail.result('get_project_task_list') or [])
                if (t or {}).get('project_work_force_flag') == 'N'
            ],
            endpoint='/services/TaskService1.svc/BulkUpdateResourceAssignments',
            data=get_all_task_resources
        )
        
        def get_task_estimate_parameters(task_uri):
            return [
                {
                    "estimateUri": r.get('estimateUri') or None,
                    "resourceUri": u['userUri'],
                    "projectRoleUri": r['roleuri'],
                    "initialEstimatedHours": None,
                    "parameterCorrelationId": None
                }
                for t in _missing_estimates_by_task().get(task_uri, [])
                for u in (t.get('users') or [])
                for r in ([next((r for r in (u.get('roleinfo') or []) if r.get('roleuri')), None)] or [])
                if r
            ]

        def get_tasks_with_estimates():
            # Only call the service for tasks that produce at least one
            # estimateParameters record; tasks with no estimates are ignored.
            return [
                t for t in (rail.result('get_project_task_list') or [])
                if get_task_estimate_parameters(t['taskUri'])
            ]

        bulk_update_task_resource_estimates = rail.RepliconServiceCallForEachItemOperator(
            task_id='bulk_update_task_resource_estimates',
            items=get_tasks_with_estimates,
            endpoint='/services/TaskService1.svc/BulkUpdateTaskResourceEstimates',
            data=lambda item: {
                "taskTarget": {
                    "uri": item['taskUri'],
                    "name": None,
                    "parent": None,
                    "project": None,
                    "parameterCorrelationId": None
                },
                "estimateParameters": get_task_estimate_parameters(item['taskUri']),
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ result('create_log') }}",
            message="na",
            severity="Error",
            items=lambda: rail.get_dag_run_conf()['item']['data'],
            properties={
                "proj_id": "{{ item.row.data.PROJ_ID }}",
                "proj_name":  "{{ item.row.data.PROJ_NAME }}",
                "action": "{{ 'Add'  if result('get_project_basic_details') | is_falsy else 'Update' }}",
                "status": "Error",
                "details": '{{ get_error_message() }}',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_log
        create_log >> get_project_basic_details >> pick_chose_wbs_sync
        pick_chose_wbs_sync >> rail.Label(
            'Yes') >> get_costpoint_projects_from_conf
        pick_chose_wbs_sync >> rail.Label('No') >> get_costpoint_projects >> get_costpoint_projects_from_conf >> \
            get_existing_client >> is_client_exists
        is_client_exists >> rail.Label('Yes') >> get_workforce_user_costpoint
        is_client_exists >> rail.Label('No') >> create_root_project_client >> get_workforce_user_costpoint
        get_workforce_user_costpoint >> get_billing_rates_costpoint >> get_users_from_replicon >> \
            is_project_role_assigment_enabled
        is_project_role_assigment_enabled >> rail.Label('Yes') >> get_project_roles_costpoint >> \
            get_pm_comanager_candidates >> get_pm_comanager_users >> resolve_pm_comanagers >> \
            log_pm_comanager_skips >> if_pm_uris_present
        if_pm_uris_present >> rail.Label('Yes') >> get_pm_comanager_current_permissions >> \
            filter_pm_uris_for_permission >> log_pm_permission_skips >> \
            assign_pm_comanager_permission >> get_task_list_info
        if_pm_uris_present >> rail.Label('No') >> get_task_list_info
        is_project_role_assigment_enabled >> rail.Label('No') >> get_project_leader_info_from_replicon >> \
            if_project_leader_uri_present
        if_project_leader_uri_present >> rail.Label('Yes') >> get_project_leader_current_permissions >> \
            filter_project_leader_uri_for_permission >> log_project_leader_permission_skips >> \
            assign_project_leader_permission >> get_task_list_info
        if_project_leader_uri_present >> rail.Label('No') >> get_task_list_info
        get_task_list_info >> get_task_info_from_replicon >> rename_tasks >> if_project_present
        if_project_present >> rail.Label(
            'Yes') >> update_project >> update_task >> project_setup_complete
        if_project_present >> rail.Label('No') >> check_should_create
        check_should_create >> rail.Label('Yes') >> add_project_and_task >> \
            update_manage_project >> update_division >> update_task >> project_setup_complete
        check_should_create >> rail.Label('No') >> finish
        project_setup_complete >> has_co_managers_to_assign
        has_co_managers_to_assign >> rail.Label('Yes') >> assign_comanager_to_project >> get_project_task_list
        has_co_managers_to_assign >> rail.Label('No') >> get_project_task_list
        get_project_task_list >> get_existing_task_resource_estimates >> get_workforce_resources >> get_resources_delete >> is_delete_resource
        is_delete_resource >> rail.Label(
            'Yes') >> bulk_delete_task_resource_estimates >> bulk_delete_resource_assignments >> bulk_delete_project_team_members_assignment >> get_missing_task_resource_estimates >> has_missing_task_resource_estimates
        is_delete_resource >> rail.Label(
            'No') >> get_missing_task_resource_estimates >> has_missing_task_resource_estimates
        has_missing_task_resource_estimates >> rail.Label(
            'Yes') >> assign_project_team_members >> assign_task_resources >> bulk_update_task_resource_estimates >> add_log_entry >> finish
        has_missing_task_resource_estimates >> rail.Label(
            'No') >> need_to_assign_all_users
        need_to_assign_all_users >> rail.Label(
            'Yes') >> assign_project_all_members >> assign_task_all_resources >> add_log_entry
        need_to_assign_all_users >> rail.Label(
            'No') >> add_log_entry
        finish >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)

import uuid
from urllib.parse import quote

import rail

from azenta.oracle_project_sync import config
from azenta.oracle_project_sync.mappers import field_mapper
from azenta.oracle_project_sync.utils import custom_methods


# ===========================================================================
# Oracle Fusion REST endpoints
# ===========================================================================

def oracle_projects_delta_endpoint(oracle_api_base, status_code, query_watermark):
    """Delta list of projects in a given Oracle status changed since the watermark."""
    encoded_watermark = quote(query_watermark, safe='')
    return (
        f"{oracle_api_base}/projects"
        f"?q=ProjectStatusCode='{status_code}';LastUpdateDate >= "
        f"'{encoded_watermark}'"
        f"&fields={field_mapper.PROJECT_FIELDS}"
        "&orderBy=LastUpdateDate:asc"
        f"&limit={config.ORACLE_PAGE_LIMIT}&onlyData=true"
    )


def oracle_project_detail_endpoint(oracle_api_base):
    """Full field set for a single project (child pulls detail by ProjectId from conf)."""
    return (
        f"{oracle_api_base}/projects/"
        "{{ dag_run.conf.ProjectId }}"
        f"?fields={field_mapper.PROJECT_FIELDS}&onlyData=true"
    )


def oracle_classifications_endpoint(oracle_api_base, project_id):
    """Project classifications (used for the CUSP - POC gate)."""
    return (
        f"{oracle_api_base}/projects/"
        f"{project_id}/child/ProjectClassifications"
        "?fields=ClassCode,ClassCategory"
        f"&limit={config.ORACLE_PAGE_LIMIT}&onlyData=true"
    )


def oracle_project_team_members_endpoint(oracle_api_base, project_id):
    """Project team members (used to resolve the active Project Manager)."""
    return (
        f"{oracle_api_base}/projects/"
        f"{project_id}/child/ProjectTeamMembers"
        "?fields=PersonEmail,PersonName,ProjectRole,StartDate,FinishDate,PersonId"
        f"&limit={config.ORACLE_PAGE_LIMIT}&onlyData=true"
    )


def oracle_tasks_endpoint(oracle_api_base, project_id):
    """All tasks for the project, any level (WBS hierarchy needs every level, not just
    chargeable leaves - a non-chargeable level-1 task is often the real parent of
    chargeable children underneath it)."""
    return (
        f"{oracle_api_base}/projects/"
        f"{project_id}/child/Tasks"
        f"?fields={field_mapper.TASK_FIELDS}"
        "&orderBy=TaskNumber:asc"
        f"&limit={config.ORACLE_PAGE_LIMIT}&onlyData=true"
    )


def oracle_financial_plans_endpoint(oracle_api_base, project_id):
    """Financial project plan versions for the project (to find the plan version id)."""
    return (
        f"{oracle_api_base}/financialProjectPlans"
        f"?q=ProjectId={project_id}"
        "&fields=PlanVersionId,PlanVersionStatus,ProjectId"
        f"&limit={config.ORACLE_PAGE_LIMIT}&onlyData=true"
    )


def oracle_plan_assignments_endpoint(oracle_api_base, plan_version_id):
    """Labor resource assignments (with planned amounts) for the resolved plan version."""
    return (
        f"{oracle_api_base}/financialProjectPlans/"
        f"{plan_version_id}/child/ResourceAssignments"
        "?q=ResourceClass='Labor'"
        "&expand=PlanningAmounts"
        "&fields=PlanningElementId,TaskId,TaskNumber,TaskName,RbsElementId,"
        "ResourceName,ResourceClass,PlanningStartDate,PlanningFinishDate,"
        "UnitOfMeasure,PlanningAmounts"
        f"&limit={config.ORACLE_PAGE_LIMIT}&onlyData=true"
    )


# ===========================================================================
# Replicon REST payloads
# ===========================================================================

def get_project_oef_uris_by_name(oef_details, name):
	"""Derive {definition_uri, column_uri, filter_uri} for a named project OEF.

	Column/filter URIs share the definition URI's GUID; only the entity-type
	segment differs (confirmed: definition/column/filter share the same GUID
	across all three URI forms).
	"""
	definition_uri = rail.find_first_by_attr_and_get_attr(oef_details, 'name', name, 'uri')
	if not definition_uri:
		raise ValueError(f'Project extension field "{name}" not found in the tenant.')
	column_uri = definition_uri.replace(':object-extension-tag-definition:', ':project-object-extension-column:')
	filter_uri = definition_uri.replace(':object-extension-tag-definition:', ':project-object-extension-filter:')
	if column_uri == definition_uri or filter_uri == definition_uri:
		raise ValueError(
			f'OEF definition URI "{definition_uri}" for "{name}" has unexpected shape '
			f'— cannot derive column/filter URIs by segment replacement'
		)
	return {'definition_uri': definition_uri, 'column_uri': column_uri, 'filter_uri': filter_uri}


def _build_oef_list(oef_details, project, is_new):
    """Build objectExtensionFieldsToApply entries for the project upsert.

    ADD-AND-UPDATE OEFs (Oracle Business Unit) are always written.
    ADD-only OEFs (Oracle Project Id, Oracle Project Type, Oracle Business Unit Id,
    Oracle Project Classification) are written only when is_new=True (first create).

    oef_details is the get_project_oef_details result (GetAllObjectExtensionFieldDetails,
    unwrapped list of {name, slug, uri, ...}) - each field's uri is looked up by its
    Replicon display name, never hardcoded.
    """
    field_values = {
        'oracle_project_id': str(project.get('ProjectId') or ''),
        'oracle_project_type': project.get('ProjectTypeName') or '',
        'oracle_business_unit_id': str(project.get('BusinessUnitId') or ''),
        'oracle_project_classification': field_mapper.REQUIRED_PROJECT_CLASSIFICATION_CODE,
        'oracle_business_unit': project.get('BusinessUnitName') or '',
    }
    name_map = dict(field_mapper.PROJECT_OEF_ADD_UPDATE_NAMES)
    if is_new:
        name_map.update(field_mapper.PROJECT_OEF_ADD_ONLY_NAMES)

    oef_list = []
    for key, oef_name in name_map.items():
        value = field_values.get(key)
        if not value:
            continue
        oef_uri = rail.find_first_by_attr_and_get_attr(oef_details, 'name', oef_name, 'uri')
        if not oef_uri:
            continue
        oef_list.append({
            'definition': {'uri': oef_uri},
            'tag': None,
            'numericValue': None,
            'textValue': value,
            'fileValue': None,
            'jsonValue': None,
        })
    return oef_list




def build_project_modifications():
    """Build the CreateProjectOrApplyModifications payload from the Oracle project detail.

    Target-toggle pattern (verified in addsystems and CRL integrations):
    - Update: target has the existing project's uri only (no code/name/correlationId).
    - Create: target is None (omitted); code and name come from codeToApply/nameToApply.

    PM is attached only when resolve_pm produced a user uri. Project name/code are ADD-only per
    spec: on create from Oracle, on update kept as-is from the existing Replicon values.

    OEFs are embedded directly in objectExtensionFieldsToApply, with each definition.uri
    resolved from get_project_oef_details (GetAllObjectExtensionFieldDetails):
    - ADD-AND-UPDATE (Oracle Business Unit): always written.
    - ADD-only (Oracle Project Id, Oracle Project Type, Oracle Business Unit Id,
      Oracle Project Classification): written on first create only.
    """
    project = rail.result('get_project_detail') or {}
    oef_details = rail.result('get_project_oef_details') or []
    replicon_status = custom_methods.map_project_status(project.get('ProjectStatusCode'))
    pm_uri = rail.result('resolve_pm')
    existing = rail.result('get_project_by_oracle_project_id')
    target = {'uri': existing['uri']} if existing else None
    is_new = existing is None

    modifications = {
        'statusToApply': {'name': replicon_status},
        'descriptionToApply': {'value': project.get('ProjectDescription') or ''},
        'keyValuesToApply': [
            {
                'keyUri': field_mapper.PROJECT_MANAGEMENT_TYPE_KEY_URN,
                'value': {'uri': field_mapper.PROJECT_MANAGEMENT_TYPE_URN},
            }
        ],
        'objectExtensionFieldsToApply': _build_oef_list(oef_details, project, is_new),
    }

    # Code and name: on create from Oracle, on update echoed back from Replicon (never overwritten).
    if is_new:
        modifications['codeToApply'] = {'value': project.get('ProjectNumber')}
        modifications['nameToApply'] = {'value': custom_methods.build_project_name(project)}
    else:
        modifications['codeToApply'] = {'value': existing.get('code')}
        modifications['nameToApply'] = {'value': existing.get('name')}

    start_date_parts = custom_methods.to_replicon_date_parts(project.get('ProjectStartDate'))
    if start_date_parts:
        modifications['startDateToApply'] = {'date': start_date_parts}
    end_date_parts = custom_methods.to_replicon_date_parts(project.get('ProjectEndDate'))
    if end_date_parts:
        modifications['endDateToApply'] = {'date': end_date_parts}
    if pm_uri:
        modifications['projectLeaderToApply'] = {'user': {'uri': pm_uri}}

    return {
        'target': target,
        'modifications': modifications,
        'projectModificationOptionUri': field_mapper.PROJECT_MODIFICATION_SAVE_URN,
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_task_oef_uris_by_name(oef_details, name):
	"""Derive {definition_uri, column_uri, filter_uri} for a named task OEF.

	Column/filter URIs share the definition URI's GUID; only the entity-type
	segment differs (confirmed: definition/column/filter share the same GUID
	across all three URI forms).
	"""
	definition_uri = rail.find_first_by_attr_and_get_attr(oef_details, 'name', name, 'uri')
	if not definition_uri:
		raise ValueError(f'Task extension field "{name}" not found in the tenant.')
	column_uri = definition_uri.replace(':object-extension-tag-definition:', ':task-object-extension-column:')
	filter_uri = definition_uri.replace(':object-extension-tag-definition:', ':task-object-extension-filter:')
	if column_uri == definition_uri or filter_uri == definition_uri:
		raise ValueError(
			f'OEF definition URI "{definition_uri}" for "{name}" has unexpected shape '
			f'— cannot derive column/filter URIs by segment replacement'
		)
	return {'definition_uri': definition_uri, 'column_uri': column_uri, 'filter_uri': filter_uri}


def get_task_by_oracle_task_id_payload(item):
	"""TaskListService1.svc/GetData: look up an existing task by its Oracle Task Id
	OEF value (Oracle's stable PK), instead of relying solely on PutTask's
	name + parent-chain matching.
	`item` = one row from build_ordered_task_worklist (has 'TaskId').
	"""
	oef_details = rail.result('get_task_oef_details') or []
	oef_uris = get_task_oef_uris_by_name(oef_details, field_mapper.TASK_OEF_ORACLE_TASK_ID_NAME)
	return {
		'page': '1',
		'pagesize': '100',
        'columnUris': [
			'urn:replicon:task-list-column:task',
			'urn:replicon:task-list-column:code',
			'urn:replicon:task-list-column:name',
			oef_uris['column_uri'],
		],
		'sort': [],
		'filterExpression': {
			'leftExpression': {
				'filterDefinitionUri': oef_uris['filter_uri'],
			},
			'operatorUri': 'urn:replicon:filter-operator:equal',
			'rightExpression': {
				'value': {
					'text': str(item.get('TaskId') or ''),
				},
			},
		},
	}


def get_task_payload(item, dag_run):
    """Build a ProjectService1.svc/PutTask payload for one Oracle WBS task (upsert).

    When an existing task is found (by Oracle Task Id OEF), update it in place:
    target is the existing task's uri, name is echoed from Replicon (frozen, never
    overwritten), code syncs from Oracle. On create, target resolves by name+parent chain
    (as today), name comes from Oracle, code from Oracle.

    The parent is resolved BY NAME using the FULL nested ancestor chain (item['parent_chain']
    - None at level 1, otherwise `{name, parent: {...}}` down to the root). A nested parent
    can't be located by its immediate name alone (PutTask 400s 'Project and Task's Project
    mis-match'), hence the whole chain. Because the worklist is ordered parent-before-child,
    every ancestor already exists by the time a child is sent.

    project_uri comes from the dag_run conf (this runs in the dedicated task DAG, which is
    triggered with the already-upserted project uri), not from a rail.result lookup.
    """
    project_uri = dag_run.conf.get('project_uri')
    existing = rail.result('get_task_by_oracle_id')
    is_new = existing is None

    if is_new:
        target = {
            'name': item['TaskName'],
            'parent': item.get('parent_chain'),
        }
        name = item['TaskName']
    else:
        target = {'uri': existing['uri'],"name": existing['name']}
        name = existing['name']

    payload = {
        'project': {'uri': project_uri},
        'task': {
            'target': target,
            'name': name,
            'code': item['TaskNumber'],
            'timeEntryDateRange': {
                'startDate': custom_methods.to_replicon_date_parts(item.get('TaskStartDate')),
                'endDate': custom_methods.to_replicon_date_parts(item.get('TaskFinishDate')),
            },
            'percentCompleted': str(item.get('TaskPercentComplete') or 0),
            'isTimeEntryAllowed': '1' if item.get('ChargeableFlag') else '0',
            'isClosed': '0',
        }
    }
    return payload


def search_user_by_login_payload():
    """UserListService1.svc/GetData payload: find a user by email (== login-name).

    `user` column first so cells[0]['uri'] is the user URI regardless of URI format.
    Returns an empty row set when no user is found so has_pm_user handles it gracefully.
    """
    return {
        'page': '1',
        'pagesize': '100',
        'columnUris': [
            'urn:replicon:user-list-column:user',
            'urn:replicon:user-list-column:login-name',
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:user-list-filter:text',
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {'text': rail.result('pick_pm_email')},
            },
        },
    }


def get_assigned_permissions_payload():
    """PermissionSetService1.svc/GetAssignedPermissionSetsForUser2 payload."""
    return {'userUri': rail.result('search_pm_user')}


def get_project_by_oracle_project_id_payload(dag_run):
    """ProjectListService1.svc/GetData: look up an existing project by its
    Oracle Project Id OEF value (Oracle's stable PK), instead of by code/name.
    """
    oef_details = rail.result('get_project_oef_details') or []
    oef_uris = get_project_oef_uris_by_name(oef_details, field_mapper.PROJECT_OEF_ADD_ONLY_NAMES['oracle_project_id'])
    return {
        'page': '1',
        'pagesize': '10',
        'columnUris': [
            'urn:replicon:project-list-column:project',
            'urn:replicon:project-list-column:code',
            'urn:replicon:project-list-column:name',
            oef_uris['column_uri'],
        ],
        'sort': [],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': oef_uris['filter_uri'],
            },
            'operatorUri': 'urn:replicon:filter-operator:equal',
            'rightExpression': {
                'value': {
                    'text': str(dag_run.conf.get('ProjectId') or ''),
                },
            },
        },
    }


# ===========================================================================
# Polaris GraphQL payloads  
# ===========================================================================

def put_task_resource_groups_oef_mutation(item):
    """Set the pipe-separated Resource Groups OEF on a task. `item` = {'task_uri', 'task_name', 'value'}.

    Uses updateTask with a minimal input (taskUri, name, extensionFieldValues only) rather than
    the targeted updateExtensionField2 mutation - confirmed working in the tenant's own GraphQL
    Playground with exactly this minimal shape (no need for the full field set - code,
    isTimeEntryAllowed, initialEstimatedCost, etc.

    `definition.uri` (not `.slug`) is confirmed by the same tenant-verified curl. The uri is
    resolved at runtime (not hardcoded) from get_task_oef_details, same pattern as the project
    OEFs in _build_oef_list.
    """
    oef_details = rail.result('get_task_oef_details') or []
    definition_uri = rail.find_first_by_attr_and_get_attr(
        oef_details, 'name', field_mapper.TASK_OEF_RESOURCE_GROUPS_NAME, 'uri')
    if not definition_uri:
        raise ValueError(
            f'Task extension field "{field_mapper.TASK_OEF_RESOURCE_GROUPS_NAME}" '
            'not found in the tenant; cannot write Resource Groups.')
    return {
        'query': (
            'mutation UpdateTask($updateTaskInput: UpdateTaskInput!) {'
            '  updateTask(input: $updateTaskInput) {'
            '    task { id extensionFieldValues { textValue definition { displayText } } }'
            '    error { type reason }'
            '  }'
            '}'
        ),
        'variables': {
            'updateTaskInput': {
                'taskUri': item['task_uri'],
                'name': item['task_name'],
                'extensionFieldValues': [
                    {
                        'definition': {'uri': definition_uri},
                        'textValue': item['value'],
                    }
                ],
            }
        },
    }


def put_task_oracle_id_oef_mutation(item, task_uri):
	"""Set the Oracle Task Id OEF on a newly-created task. `item` = one row from
	build_ordered_task_worklist (has 'TaskId'), `task_uri` = result of put_task.

	Uses updateTask with minimal input (taskUri, name, extensionFieldValues only),
	same shape as put_task_resource_groups_oef_mutation.
	"""
	if not task_uri:
		raise ValueError(
			f'Cannot set Oracle Task ID OEF: PutTask failed to return a task URI. '
			f'Oracle Task ID={item.get("TaskId")} Task Name={item.get("TaskName")}'
		)
	oef_details = rail.result('get_task_oef_details') or []
	definition_uri = rail.find_first_by_attr_and_get_attr(
		oef_details, 'name', field_mapper.TASK_OEF_ORACLE_TASK_ID_NAME, 'uri')
	if not definition_uri:
		raise ValueError(
			f'Task extension field "{field_mapper.TASK_OEF_ORACLE_TASK_ID_NAME}" '
			'not found in the tenant; cannot write Oracle Task Id.')
	return {
		'query': (
			'mutation UpdateTask($updateTaskInput: UpdateTaskInput!) {'
			'  updateTask(input: $updateTaskInput) {'
			'    task { id extensionFieldValues { textValue definition { displayText } } }'
			'    error { type reason }'
			'  }'
			'}'
		),
		'variables': {
			'updateTaskInput': {
				'taskUri': task_uri,
				'name': item['TaskName'],
				'extensionFieldValues': [
					{
						'definition': {'uri': definition_uri},
						'textValue': str(item.get('TaskId') or ''),
					}
				],
			}
		},
	}


def put_project_role_payload():
    """Create a project role named after the current placeholder item's resource group.

    REST ProjectRoleService1.svc/PutProjectRole, target.uri=null + target.name=<name> upsert-by-
    name pattern - same shape used by every PutProjectRole caller in this repo (merrick's
    create_discipline_roles, neology's process_create_projectroles_child, deltek_costpoint_
    polaris's roles_child_dag). Unlike those callers, we do need the uri back in the same run
    (see put_task_resource_estimate_mutation) - read directly off this call's own response
    (RepliconServiceOperator returns the parsed entity dict, same as upsert_project's `.uri`
    access), not via a second GetActiveRoles re-fetch.
    """
    item = rail.result('for_each_placeholder')
    name = item['resource_name']
    return {
        'projectRoleUri': {
            'target': {'uri': None, 'name': name},
            'name': name,
            'description': None,
            'isArchived': 'false',
            'isBillable': 'true',
            'rateSchedule': None,
        }
    }


def get_task_resource_estimates_query():
    """Query existing role estimates for the current task (drives update vs. create in put_task_resource_estimate_mutation)."""
    item = rail.result('for_each_placeholder')
    return {
        'query': (
            'query getTaskResourceEstimatesForTask($taskId: String!, $page: Int!, $pageSize: Int!) {'
            '  task(taskId: $taskId) {'
            '    resourceEstimates(page: $page, pageSize: $pageSize) {'
            '      id'
            '      projectRole { id }'
            '      resourceUser { id }'
            '    }'
            '  }'
            '}'
        ),
        'variables': {
            'taskId': item['task_uri'],
            'page': 1,
            'pageSize': 10000,
        },
    }


def get_task_resource_estimates_query_by_task(task_uri):
    """Query existing role estimates for an explicit task URI.

    Same shape as get_task_resource_estimates_query() but accepts task_uri as an argument.
    Used by post-loop cleanup phase (get_task_estimates_for_cleanup) where results come from
    RepliconServiceCallForEachItemOperator, not from inside for_each_placeholder loop.
    """
    return {
        'query': (
            'query getTaskResourceEstimatesForTask($taskId: String!, $page: Int!, $pageSize: Int!) {'
            '  task(taskId: $taskId) {'
            '    name'
            '    resourceEstimates(page: $page, pageSize: $pageSize) {'
            '      id'
            '      projectRole { id displayText }'
            '      resourceUser { id }'
            '    }'
            '  }'
            '}'
        ),
        'variables': {
            'taskId': task_uri,
            'page': 1,
            'pageSize': 10000,
        },
    }
    


def put_task_resource_estimate_mutation():
    """Upsert a role placeholder estimate for the current item.

    Queries ph_get_task_estimates first: if an estimate for this role already exists on
    the task, passes its taskResourceEstimateId so Polaris updates it (prevents duplicates
    on re-runs). If none exists, omits the ID so Polaris creates a new placeholder.
    No user/allocation block — PM fills in the resource assignment manually.
    """
    item = rail.result('for_each_placeholder')
    role_uri = rail.result('ph_resolve_role_uri') or (rail.result('ph_create_role') or {}).get('uri')

    estimates = (
        (rail.result('ph_get_task_estimates') or {})
        .get('data', {}).get('task', {}).get('resourceEstimates') or []
    )
    existing = next(
        (e for e in estimates if (e.get('projectRole') or {}).get('id') == role_uri), None
    )

    estimate_input = {
        'taskId': item['task_uri'],
        'projectRoleId': role_uri,
    }
    if existing:
        estimate_input['taskResourceEstimateId'] = existing['id']
    if item.get('planned_qty') is not None:
        estimate_input['initialEstimatedHours'] = float(item['planned_qty'])

    return {
        'query': (
            'mutation putTaskResourceEstimate'
            '($input: PutTaskResourceEstimateInput!) {'
            '  putTaskResourceEstimate(input: $input) {'
            '    taskResourceEstimateId'
            '  }'
            '}'
        ),
        'variables': {'input': estimate_input},
    }


def remove_task_resource_estimate_mutation(item):
    """Delete a role placeholder estimate from a task.

    Called once per orphaned estimate (an estimate whose role no longer exists in Oracle's
    placeholder list for this task, and which has no PM-assigned real resource).
    Expects item dict with keys 'taskId' and 'taskResourceEstimateId'.
    """
    return {
        'query': (
            'mutation removeTaskResourceEstimate'
            '($input: RemoveTaskResourceEstimateInput!) {'
            '  removeTaskResourceEstimate(input: $input) {'
            '    taskResourceEstimateId'
            '  }'
            '}'
        ),
        'variables': {
            'input': {
                'taskId': item['taskId'],
                'taskResourceEstimateId': item['taskResourceEstimateId'],
            }
        },
    }


# ===========================================================================
# Log properties
# ===========================================================================

def _base_props():
    return {
        'project_id': '{{ dag_run.conf.ProjectId }}',
        'jobid': '{{ dag_run_ecid() }} | {{ dag_run.run_id }}',
    }


def _create_or_update_action():
    """Return 'Create' when no existing Replicon project was found, 'Update' otherwise."""
    existing = rail.result('get_project_by_oracle_project_id')
    return 'Update' if existing else 'Create'


def status_excluded_log_properties():
    """Exception entry: Oracle status (DRAFT/REJECTED) excludes this project from sync."""
    props = _base_props()
    props.update({
        'action': 'Validation',
        'status': 'Exception',
        'details': (
            "Project excluded from sync - Oracle status '{{ dag_run.conf.ProjectStatusCode }}' "
            "is not eligible for integration (DRAFT/REJECTED); accepted and recorded for audit."
        ),
    })
    return props


def classification_excluded_log_properties():
    """Exception entry: project is missing the required CUSP-POC classification."""
    props = _base_props()
    props.update({
        'action': 'Validation',
        'status': 'Exception',
        'details': (
            "Project excluded from sync - required 'CUSP - POC' project classification not "
            "found on the Oracle project; accepted and recorded for audit."
        ),
    })
    return props


def success_log_properties():
    """Audit entry for a successfully synced project."""
    props = _base_props()
    props.update({'action': _create_or_update_action(), 'status': 'Success'})
    return props


def pm_missing_log_properties():
    """Exception entry when the project manager could not be resolved/assigned."""
    props = _base_props()
    props.update({
        'action': _create_or_update_action(),
        'status': 'Exception',
        'details': ('Project was synced but project manager was not assigned - '
                    'no active PM in Oracle, email not found in Polaris, or user lacks '
                    'project-management permission'),
    })
    return props


def error_log_properties():
    """Audit entry for a failed project sync."""
    props = _base_props()
    props.update({
        'action': 'Create',
        'status': 'Error',
        'details': '{{ get_error_message() }}',
    })
    return props

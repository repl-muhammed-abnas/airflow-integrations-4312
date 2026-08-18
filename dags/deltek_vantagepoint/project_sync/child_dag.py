from airflow.models import Variable
from datetime import timedelta
from copy import deepcopy
import uuid as _uuid
import itertools
import rail

from deltek_vantagepoint.project_sync.utils import request_payload, python_callable_method


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_{config.region.replace("-", "_")}_project_sync_child_{config.company_key}',
        description=f'Deltek Vantagepoint project and task sync {config.company_key}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='fetch_project_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_project_details',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        fetch_project_details = rail.RepliconServiceOperator(
            task_id='fetch_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda dag_run: { 'projects': [{ 'code': dag_run.conf.get('WBSNumber', dag_run.conf['WBS1']) }] }
        )

        def _labor_code_sync_enabled():
            raw = getattr(config, 'enable_budget_labor_codes_level', False)
            if isinstance(raw, str):
                raw = raw.strip().lower() == 'true'
            return bool(raw)

        def _should_sync_timesheet_lc(dag_run):
            if not _labor_code_sync_enabled():
                return False
            if getattr(config, 'budget_labor_codes_level', '') != 'TimesheetFields':
                return False
            return dag_run.conf.get('Action') != dag_run.conf.get('WEBHOOK_ACTION', {}).get('DELETE')

        def _project_resource_sync_enabled():
            raw = getattr(config, 'project_resource_enabled', False)
            if isinstance(raw, str):
                raw = raw.strip().lower() == 'true'
            return bool(raw) and _labor_code_sync_enabled() and getattr(config, 'budget_labor_codes_level', '') == 'Task'

        def _get_project_resource_uris():
            if not _project_resource_sync_enabled():
                return []
            for task_id in ('fetch_replicon_project_resources_lc', 'fetch_replicon_project_resources'):
                try:
                    response = rail.result(task_id)
                    rows = response.get('d', {}).get('rows', [])
                    return [row['cells'][0]['uri'] for row in rows if row.get('cells')]
                except Exception:
                    continue
            return []

        is_labor_code_sync_enabled = rail.IfOperator(
            task_id='is_labor_code_sync_enabled',
            test=_labor_code_sync_enabled,
            yes_task='fetch_labor_codes',
            no_task='if_project_exists'
        )

        fetch_labor_codes = rail.VantagepointAPIOperator(
            task_id='fetch_labor_codes',
            endpoint='/project/BudgetWorksheet/Labor',
            filters=lambda dag_run: f"?wbs1={dag_run.conf['WBS1']}",
            request_method='GET',
            pagination=False,
            vp_conn_id=config.deltek_vantagepoint_conn_id
        )

        if_project_exists = rail.IfOperator(
            task_id='if_project_exists',
            test="{{ result('fetch_project_details')[0].projectDetails | is_truthy }}",
            yes_task='fetch_task_details',
            no_task='is_phase_or_task'
        )

        def get_task_details(response):
            if not response:
                return []
            flatten_rows = list(
                itertools.chain(*list(map(lambda x: x['rows'], response)))
            )
            return list(map(lambda x: {
                'uri': x['cells'][0].get('uri'),
                'code': x['cells'][1].get('textValue'),
                'name': x['cells'][2].get('textValue'),
                'parent': x['cells'][3].get('textValue'),
                'status': x['cells'][4].get('boolValue'),
                'actualHours': x['cells'][5].get('textValue'),
                'hierarchyLevel': x['hierarchyLevel'],
                'hasChildren': x['hasChildren']
            }, flatten_rows))
        fetch_task_details = rail.RepliconServicePageOperator(
            task_id='fetch_task_details',
            endpoint='/services/TaskListService1.svc/GetHierarchyDataForProject',
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 10000,
                'project': { 'code': dag_run.conf['WBS1'] },
                'columnUris': [
                    'urn:replicon:task-list-column:task',
                    'urn:replicon:task-list-column:code',
                    'urn:replicon:task-list-column:name',
                    'urn:replicon:task-list-column:parent',
                    'urn:replicon:task-list-column:enabled',
                    'urn:replicon:task-list-column:actual-hours'
                ]
            },
            page_handler=request_payload.page_handler,
            all_result_data_handler=get_task_details
        )

        is_phase_or_task = rail.IfOperator(
            task_id='is_phase_or_task',
            test="{{ dag_run.conf.WBS2 != ' ' }}",
            yes_task='should_delete_task',
            no_task='should_sync_client'
        )

        should_delete_task = rail.IfOperator(
            task_id='should_delete_task',
            test='{{ dag_run.conf.Action == dag_run.conf.WEBHOOK_ACTION.DELETE }}',
            yes_task='has_time_entry',
            no_task='is_parent_present'
        )


        def check_time_entry_present(dag_run):
            is_subtask = dag_run.conf['WBS3'] != ' '
            if is_subtask:
                subtask_code = f"{dag_run.conf['WBS2']}/{dag_run.conf['WBS3']}"
                for x in rail.result('fetch_task_details'):
                    if x['hierarchyLevel'] == 1 and x['code'] == subtask_code:
                        if not (x['actualHours'] == '0.00' or x['actualHours'] == None):
                            return True
            else:
                task_code = dag_run.conf['WBS2']
                for x in rail.result('fetch_task_details'):
                    if x['hierarchyLevel'] == 0 and x['code'] == task_code:
                        if not (x['actualHours'] == '0.00' or x['actualHours'] == None):
                            return True

                    if x['hierarchyLevel'] == 1 and x['code'].split('/')[0] == task_code:
                        if not (x['actualHours'] == '0.00' or x['actualHours'] == None):
                            return True
            return False
        has_time_entry = rail.IfOperator(
            task_id='has_time_entry',
            test=check_time_entry_present,
            yes_task='disable_task_or_subtask',
            no_task='delete_task'
        )

        def get_task_uri(dag_run):
            is_subtask = dag_run.conf['WBS3'] != ' '
            hierarchy_level = 1 if is_subtask else 0

            task_code = f"{dag_run.conf['WBS2']}/{dag_run.conf['WBS3']}" \
                if is_subtask else dag_run.conf['WBS2']

            task = next(iter(filter(
                lambda x: x['code'] == task_code and x['hierarchyLevel'] == hierarchy_level,
                rail.result('fetch_task_details')
            )), {})
            return { 'taskUri': task.get('uri') }
        delete_task = rail.RepliconServiceOperator(
            task_id='delete_task',
            endpoint='/services/TaskService1.svc/Delete',
            data=get_task_uri
        )


        disable_task_or_subtask = rail.RepliconServiceOperator(
            task_id='disable_task_or_subtask',
            endpoint='/services/TaskService1.svc/Close',
            data=get_task_uri
        )

        write_disable_task_exception = rail.WriteLogOperator(
            task_id='write_disable_task_exception',
            message='Exceptions',
            severity='Error/Exception',
            properties=lambda: {
                'code': '{{ dag_run.conf.WBS1 }}',
                'action': '{{ dag_run.conf.Action }}',
                'status': 'Exception',
                'reason': f"Task {'{{ dag_run.conf.Name }}'} \
                    ({'{{ dag_run.conf.WBS3 }}' if '{{ dag_run.conf.WBS3 }}' != ' ' else '{{ dag_run.conf.WBS2 }}'}) has been Disabled"
            }
        )


        def check_if_parent_exists(dag_run):
            if dag_run.conf['WBS3'] == ' ' or not rail.result('fetch_task_details'):
                return True

            for task in rail.result('fetch_task_details'):
                if task['hierarchyLevel'] == 0 and task['code'] == dag_run.conf['WBS2']:
                    return True
            return False
        is_parent_present = rail.IfOperator(
            task_id='is_parent_present',
            test=check_if_parent_exists,
            yes_task='is_lc_project_resource_enabled',
            no_task='write_parent_exception'
        )
        write_parent_exception = rail.WriteLogOperator(
            task_id='write_parent_exception',
            message='Exceptions',
            severity='Error/Exception',
            properties={
                'code': '{{ dag_run.conf.WBS1 }}',
                'action': '{{ dag_run.conf.Action }}',
                'status': 'Exception',
                'reason': 'Phase ({{ dag_run.conf.WBS2 }}) is not present while syncing Task ({{ dag_run.conf.WBS3 }})'
            }
        )


        def get_update_task_details_param(dag_run):
            param = request_payload.get_task_modifications_param(dag_run)
            resource_sync_enabled = _project_resource_sync_enabled()
            resource_uris = _get_project_resource_uris() if resource_sync_enabled else []
            if resource_uris:
                param['modifications']['resourceAssignmentModifications'] = {
                    'resourcesToAdd': [{'user': {'uri': uri}} for uri in resource_uris]
                }
            elif resource_sync_enabled:
                param['modifications']['resourceAssignmentModifications'] = {
                    'resourcesToAdd': []
                }
            return param

        update_task_details = rail.RepliconServiceOperator(
            task_id='update_task_details',
            endpoint='/services/TaskService1.svc/CreateTaskOrApplyModifications',
            data=get_update_task_details_param
        )

        def should_sync_labor_codes_test():
            try:
                result = rail.result('fetch_labor_codes')
                labor_codes = result.get('data', []) if isinstance(result, dict) else result
                return (
                    _labor_code_sync_enabled()
                    and getattr(config, 'budget_labor_codes_level', '') == 'Task'
                    and bool(labor_codes)
                )
            except Exception:
                return False

        should_sync_labor_codes = rail.IfOperator(
            task_id='should_sync_labor_codes',
            test=should_sync_labor_codes_test,
            yes_task='prepare_labor_code_tasks',
            no_task='should_sync_timesheet_lc_phasetask'
        )

        def _parse_labor_code_entry(entry):
            wbs2 = (entry.get('WBS2') or '').strip()
            wbs3 = (entry.get('WBS3') or '').strip()
            return {
                'labor_code': entry['LaborCode'],
                'labor_code_name': f"{entry['LaborCode']}-{entry['LaborCodeName']}",
                'date_range': request_payload.getTimeEntryDateRange(
                    entry.get('StartDate'), entry.get('EndDate')
                ),
                'parent_code': f"{wbs2}/{wbs3}" if wbs3 else wbs2 if wbs2 else None
            }

        def _labor_code_wbs_codes(labor_codes):
            codes = set()
            for entry in (labor_codes or []):
                wbs2 = (entry.get('WBS2') or '').strip()
                wbs3 = (entry.get('WBS3') or '').strip()
                if wbs2:
                    codes.add(wbs2)
                    if wbs3:
                        codes.add(f"{wbs2}/{wbs3}")
            return codes

        def _is_labor_code_task(task):
            code = task.get('code') or ''
            name = task.get('name') or ''
            return bool(code) and name.startswith(f"{code}-")

        def _close_missing_tasks_hierarchy(payload, existing_tasks, labor_codes):
            payload_codes = {
                m['taskModificationToApply']['codeToApply']['value']
                for m in payload.get('taskHierarchy', [])
                if m.get('taskModificationToApply', {}).get('codeToApply', {}).get('value')
            }
            wbs_codes = _labor_code_wbs_codes(labor_codes)
            for task in (existing_tasks or []):
                code = task.get('code')
                if code and code not in payload_codes and code not in wbs_codes \
                        and _is_labor_code_task(task):
                    payload['taskHierarchy'].append({
                        'target': { 'uri': task['uri'] },
                        'taskModificationToApply': {
                            'name': task['name'],
                            'codeToApply': { 'value': task['code'] },
                            'isClosed': True,
                            'isTimeEntryAllowed': False,
                        }
                    })
            return payload

        def _close_missing_tasks_put_project(task_hierarchy, existing_tasks, labor_codes):
            def collect_codes(tasks):
                codes = set()
                for item in tasks:
                    code = item.get('task', {}).get('code')
                    if code:
                        codes.add(code)
                    codes.update(collect_codes(item.get('childTasks', [])))
                return codes

            payload_codes = collect_codes(task_hierarchy)
            wbs_codes = _labor_code_wbs_codes(labor_codes)
            for task in (existing_tasks or []):
                code = task.get('code')
                if code and code not in payload_codes and code not in wbs_codes \
                        and _is_labor_code_task(task):
                    task_hierarchy.append({
                        'task': {
                            'target': { 'uri': task['uri'] },
                            'name': task['name'],
                            'code': task['code'],
                            'isClosed': True,
                            'isTimeEntryAllowed': False,
                            'assignedResources': [],
                        },
                        'childTasks': []
                    })
            return task_hierarchy

        def build_labor_code_hierarchy_payload(dag_run):
            raw_result = rail.result('fetch_labor_codes')
            labor_codes = raw_result.get('data', []) if isinstance(raw_result, dict) else raw_result
            existing_tasks = rail.result('fetch_task_details') or []
            project_code = dag_run.conf['WBS1']
            _resource_sync_enabled = _project_resource_sync_enabled()
            _resource_uris = _get_project_resource_uris() if _resource_sync_enabled else []

            existing_by_code = { t.get('code'): t for t in existing_tasks if t.get('code') }

            def project_ref():
                return {
                    'code': project_code
                }

            def build_parent_chain(ancestor_codes):
                parent = None
                for code in reversed(ancestor_codes):
                    existing = existing_by_code.get(code)
                    node = { 'name': existing['name'] if existing else code }
                    if parent is None:
                        node['project'] = project_ref()
                    else:
                        node['parent'] = parent
                    parent = node
                return parent

            task_hierarchy = []
            parent_codes = set()
            for entry in labor_codes:
                parsed = _parse_labor_code_entry(entry)
                wbs2 = (entry.get('WBS2') or '').strip()
                wbs3 = (entry.get('WBS3') or '').strip()

                ancestor_codes = []
                if wbs2:
                    ancestor_codes = [f"{wbs2}/{wbs3}", wbs2] if wbs3 else [wbs2]

                parent_node = build_parent_chain(ancestor_codes)
                labor_existing = existing_by_code.get(parsed['labor_code'])
                if labor_existing:
                    target = { 'uri': labor_existing['uri'] }
                else:
                    target = {}
                    if parent_node is not None:
                        target['parent'] = parent_node
                    else:
                        target['project'] = project_ref()

                task_hierarchy.append({
                    'target': target,
                    'taskModificationToApply': {
                        'name': parsed['labor_code_name'],
                        'codeToApply': { 'value': parsed['labor_code'] },
                        'isClosed': False,
                        'timeEntryStartDateToApply': parsed['date_range']['startDate'],
                        'timeEntryEndDateToApply': parsed['date_range']['endDate'],
                        'timeAndExpenseEntryTypeToApply': {
                            'value': 'urn:replicon:time-and-expense-entry-type:billable-and-non-billable'
                        },
                        'isTimeEntryAllowed': True,
                        'resourceAssignmentModifications': {
                            'resourcesToAdd': (
                                [{'user': {'uri': uri}} for uri in _resource_uris]
                                if _resource_uris
                                else [] if _resource_sync_enabled
                                else [{'department': {'uri': dag_run.conf['all_users_uri']}}]
                            )
                        }
                    }
                })

                if parsed['parent_code']:
                    parent_codes.add(parsed['parent_code'])

            for parent_code in parent_codes:
                parent = existing_by_code.get(parent_code)
                if not parent:
                    continue
                task_hierarchy.append({
                    'target': { 'uri': parent['uri'] },
                    'taskModificationToApply': {
                        'name': parent['name'],
                        'codeToApply': { 'value': parent['code'] },
                        'isTimeEntryAllowed': False,
                    }
                })

            payload = {
                'project': project_ref(),
                'taskHierarchy': task_hierarchy,
                'taskModificationOptionUri': 'urn:replicon:task-modification-option:save',
                'unitOfWorkId': str(_uuid.uuid4())
            }

            return _close_missing_tasks_hierarchy(payload, existing_tasks, labor_codes)

        prepare_labor_code_tasks = rail.PythonOperator(
            task_id='prepare_labor_code_tasks',
            python_callable=build_labor_code_hierarchy_payload
        )

        sync_labor_code_tasks = rail.RepliconServiceOperator(
            task_id='sync_labor_code_tasks',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            data=lambda: rail.result('prepare_labor_code_tasks')
        )

        def check_if_should_update_parent(dag_run):
            if dag_run.conf['Action'] == dag_run.conf['WEBHOOK_ACTION']['UPDATE']:
                return False
            
            is_phase = dag_run.conf['WBS3'] == ' '
            parent_phase = next(iter(filter(
                lambda x: x['hierarchyLevel'] == 0 and x['code'] == dag_run.conf['WBS2'],
                rail.result('fetch_task_details')
            )), None) if not is_phase else None

            if dag_run.conf['Action'] == dag_run.conf['WEBHOOK_ACTION']['INSERT']:
                if is_phase:
                    return len(rail.result('fetch_task_details')) == 0
                else:
                    return not parent_phase['hasChildren']

            if dag_run.conf['Action'] == dag_run.conf['WEBHOOK_ACTION']['DELETE']:
                if is_phase:
                    phase_count = len([x for x in rail.result('fetch_task_details') if x['hierarchyLevel'] == 0])
                    return phase_count == 1
                else:
                    task_count = len([
                        x for x in rail.result('fetch_task_details') \
                            if x['hierarchyLevel'] == 1 and x['parent'] == parent_phase['name']
                    ])
                    return task_count == 1
        
        should_update_parent_time_entry = rail.IfOperator(
            task_id='should_update_parent_time_entry',
            test=check_if_should_update_parent,
            yes_task='modify_parent_time_entry_params',
            no_task='catch_error'
        )

        modify_parent_time_entry_params = rail.PythonOperator(
            task_id='modify_parent_time_entry_params',
            python_callable=request_payload.get_modify_parent_time_entry_params
        )

        update_parent_time_entry = rail.RepliconServiceOperator(
            task_id='update_parent_time_entry',
            endpoint="{{ result('modify_parent_time_entry_params').url }}",
            data="{{ result('modify_parent_time_entry_params').params }}"
        )


        should_sync_client = rail.IfOperator(
            task_id='should_sync_client',
            test="{{ dag_run.conf.ClientID != '' }}",
            yes_task='sync_client',
            no_task='fetch_project_oefs'
        )


        sync_client = rail.RepliconServiceOperator(
            task_id='sync_client',
            endpoint='/services/ClientService1.svc/PutClient',
            data=lambda dag_run: {
                'client': {
                    'target': { 'code': dag_run.conf['ClientID'] },
                    'name': dag_run.conf['ClientName'],
                    'code': dag_run.conf['ClientID'],
                    'isActive': True,
                    'clientAddress': {
                        'address': dag_run.conf['ClientAddress'],
                        'zipPostalCode': dag_run.conf['ClientCityStateZip']
                    }
                }
            }
        )


        fetch_project_oefs = rail.RepliconServiceOperator(
            task_id='fetch_project_oefs',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data=lambda: { 'bindingContextUri': 'urn:replicon:object-type:project' }
        )


        fetch_replicon_users = rail.RepliconServiceOperator(
            task_id='fetch_replicon_users',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=request_payload.get_required_users,
            data_handler=lambda res: list(map(lambda x: {
                'uri': x['userDetails']['uri'],
                'loginName': x['securityConfiguration']['loginName'],
                'isEnabled': x['userDetails']['isEnabled'],
                'permissionSets': list(map(lambda x: x['uri'], x.get('permissionSets', []))),
            }, res))
        )


        def get_managers_and_comanagers(dag_run):
            should_apply_manager = list(filter(
                lambda x: x['isEnabled'] and x['loginName'] == dag_run.conf['ProjMgr'],
                rail.result('fetch_replicon_users')
            ))
            return {
                'MANAGER': should_apply_manager,
                'PRINCIPAL': python_callable_method.should_apply_user(dag_run, 'PRINCIPAL'),
                'SUPERVISOR': python_callable_method.should_apply_user(dag_run, 'SUPERVISOR')
            }
        managers_and_comanagers = rail.PythonOperator(
            task_id='managers_and_comanagers',
            python_callable=get_managers_and_comanagers
        )


        def check_if_permission_required(dag_run):
            managers = rail.result('managers_and_comanagers')
            return managers['MANAGER'] or managers['PRINCIPAL'] or managers['SUPERVISOR']
        is_manager_permission_required = rail.IfOperator(
            task_id='is_manager_permission_required',
            test=check_if_permission_required,
            yes_task='manager_permissions',
            no_task='should_assign_manager_permission'
        )


        manager_permissions = rail.RepliconServiceOperator(
            task_id='manager_permissions',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda res, dag_run: [x['uri'] for x in res if x['name'] == dag_run.conf['ROLES']['MANAGER']]
        )


        def test_if_assign_permission_required(user):
            permission_uris = rail.result('manager_permissions')
            if not user or not permission_uris:
                return False
            is_user_enabled = False
            for x in rail.result('fetch_replicon_users'):
                if x['loginName'] == user:
                    if x['isEnabled']:
                        is_user_enabled = True
                    if permission_uris[0] in x['permissionSets']:
                        return False
            return is_user_enabled
        should_assign_manager_permission = rail.IfOperator(
            task_id='should_assign_manager_permission',
            test=lambda dag_run: test_if_assign_permission_required(dag_run.conf['ProjMgr']),
            yes_task='assign_manager_permission',
            no_task='is_project_resource_enabled'
        )


        assign_manager_permission = rail.RepliconServiceOperator(
            task_id='assign_manager_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=lambda dag_run: {
                'userUri': next(x for x in rail.result('fetch_replicon_users') \
                    if x['loginName'] == dag_run.conf['ProjMgr'])['uri'],
                'permissionSetUri': rail.result('manager_permissions')[0]
            }
        )


        is_project_resource_enabled = rail.IfOperator(
            task_id='is_project_resource_enabled',
            test=_project_resource_sync_enabled,
            yes_task='fetch_vp_team_members',
            no_task='sync_project_and_task'
        )

        fetch_vp_team_members = rail.VantagepointAPIOperator(
            task_id='fetch_vp_team_members',
            endpoint='/project',
            filters=lambda dag_run: f"/{dag_run.conf['WBS1']}/teammember",
            request_method='GET',
            pagination=False,
            vp_conn_id=config.deltek_vantagepoint_conn_id
        )

        def check_employee_resources_present():
            team_members = rail.result('fetch_vp_team_members')
            if isinstance(team_members, dict):
                team_members = team_members.get('data', team_members)
            return any(
                m.get('RecordType') == 'Employee' and m.get('Employee')
                for m in (team_members or [])
            )

        is_employee_resources_present = rail.IfOperator(
            task_id='is_employee_resources_present',
            test=check_employee_resources_present,
            yes_task='fetch_replicon_project_resources',
            no_task='sync_project_and_task'
        )

        def build_project_resource_filter(dag_run):
            team_members = rail.result('fetch_vp_team_members')
            if isinstance(team_members, dict):
                team_members = team_members.get('data', team_members)
            employees = [
                m['Employee'] for m in (team_members or [])
                if m.get('RecordType') == 'Employee' and m.get('Employee')
            ]
            return request_payload.build_login_filter_payload(employees)

        fetch_replicon_project_resources = rail.RepliconServiceOperator(
            task_id='fetch_replicon_project_resources',
            endpoint='/services/UserListService1.svc/GetData',
            data=build_project_resource_filter
        )

        def check_employee_resources_present_lc():
            team_members = rail.result('fetch_vp_team_members_lc')
            if isinstance(team_members, dict):
                team_members = team_members.get('data', team_members)
            return any(
                m.get('RecordType') == 'Employee' and m.get('Employee')
                for m in (team_members or [])
            )

        def build_project_resource_filter_lc(dag_run):
            team_members = rail.result('fetch_vp_team_members_lc')
            if isinstance(team_members, dict):
                team_members = team_members.get('data', team_members)
            employees = [
                m['Employee'] for m in (team_members or [])
                if m.get('RecordType') == 'Employee' and m.get('Employee')
            ]
            return request_payload.build_login_filter_payload(employees)

        is_lc_project_resource_enabled = rail.IfOperator(
            task_id='is_lc_project_resource_enabled',
            test=_project_resource_sync_enabled,
            yes_task='fetch_vp_team_members_lc',
            no_task='should_put_project_team_members'
        )

        fetch_vp_team_members_lc = rail.VantagepointAPIOperator(
            task_id='fetch_vp_team_members_lc',
            endpoint='/project',
            filters=lambda dag_run: f"/{dag_run.conf['WBS1']}/teammember",
            request_method='GET',
            pagination=False,
            vp_conn_id=config.deltek_vantagepoint_conn_id
        )

        is_employee_resources_present_lc = rail.IfOperator(
            task_id='is_employee_resources_present_lc',
            test=check_employee_resources_present_lc,
            yes_task='fetch_replicon_project_resources_lc',
            no_task='should_put_project_team_members'
        )

        fetch_replicon_project_resources_lc = rail.RepliconServiceOperator(
            task_id='fetch_replicon_project_resources_lc',
            endpoint='/services/UserListService1.svc/GetData',
            data=build_project_resource_filter_lc
        )

        def build_labor_code_tasks_for_new_project(labor_codes, task_hierarchy, existing_tasks=None, all_users_uri=None, resource_uris=None, project_resource_sync_enabled=False):
            hierarchy = deepcopy(task_hierarchy)

            def index_tasks(tasks, lookup, level=0):
                for item in tasks:
                    code = item['task'].get('code')
                    if code:
                        lookup[code] = (item, level)
                    index_tasks(item.get('childTasks', []), lookup, level + 1)

            task_lookup = {}
            index_tasks(hierarchy, task_lookup)

            existing_by_code = {
                t.get('code'): t for t in (existing_tasks or []) if t.get('code')
            }

            for entry in labor_codes:
                parsed = _parse_labor_code_entry(entry)

                existing_task = existing_by_code.get(parsed['labor_code'])
                target = { 'uri': existing_task['uri'] } \
                    if existing_task and existing_task.get('uri') \
                    else { 'name': parsed['labor_code_name'] }

                labor_task_entry = {
                    'task': {
                        'target': target,
                        'name': parsed['labor_code_name'],
                        'code': parsed['labor_code'],
                        'isTimeEntryAllowed': True,
                        'isClosed': False,
                        'timeEntryDateRange': parsed['date_range'],
                        'assignedResources': (
                            [{'user': {'uri': uri}} for uri in resource_uris]
                            if resource_uris
                            else [] if project_resource_sync_enabled
                            else [{'department': {'uri': all_users_uri}}] if all_users_uri else []
                        )
                    },
                    'childTasks': []
                }

                if parsed['parent_code'] and parsed['parent_code'] in task_lookup:
                    parent_item, _ = task_lookup[parsed['parent_code']]
                    parent_item['task']['isTimeEntryAllowed'] = False
                    parent_item.setdefault('childTasks', []).append(labor_task_entry)
                else:
                    hierarchy.append(labor_task_entry)

            return _close_missing_tasks_put_project(hierarchy, existing_tasks, labor_codes)

        def _replace_task_resources(tasks, resource_uris):
            for item in tasks:
                item['task']['assignedResources'] = [{'user': {'uri': uri}} for uri in resource_uris]
                if item.get('childTasks'):
                    _replace_task_resources(item['childTasks'], resource_uris)

        def get_sync_project_and_task_param_with_labor_codes(dag_run):
            base_param = request_payload.get_sync_project_and_task_param(dag_run)

            resource_sync_enabled = _project_resource_sync_enabled()
            resource_uris = _get_project_resource_uris() if resource_sync_enabled else []
            if resource_uris:
                base_param['project']['team'] = {
                    'teamMembers': [{'resource': {'uri': uri}} for uri in resource_uris]
                }
                _replace_task_resources(base_param['project'].get('tasks', []), resource_uris)
            elif resource_sync_enabled:
                base_param['project']['team'] = {'teamMembers': []}
                _replace_task_resources(base_param['project'].get('tasks', []), [])

            if not _labor_code_sync_enabled():
                return base_param
            if getattr(config, 'budget_labor_codes_level', '') == 'Task':
                raw_result = rail.result('fetch_labor_codes')
                labor_codes = raw_result.get('data', []) if isinstance(raw_result, dict) else raw_result
                if labor_codes:
                    existing_tasks = rail.result('fetch_task_details') \
                        if rail.result('fetch_project_details')[0]['projectDetails'] else []
                    base_param['project']['tasks'] = build_labor_code_tasks_for_new_project(
                        labor_codes,
                        base_param['project'].get('tasks', []),
                        existing_tasks=existing_tasks or [],
                        all_users_uri=dag_run.conf.get('all_users_uri'),
                        resource_uris=resource_uris,
                        project_resource_sync_enabled=resource_sync_enabled
                    )
            return base_param

        sync_project_and_task = rail.RepliconServiceOperator(
            task_id='sync_project_and_task',
            endpoint='/services/ImportService1.svc/PutProject4',
            data=get_sync_project_and_task_param_with_labor_codes
        )


        def check_should_put_team_members(dag_run):
            if not _project_resource_sync_enabled():
                return False
            delete_action = dag_run.conf.get('WEBHOOK_ACTION', {}).get('DELETE')
            action = dag_run.conf.get('Action')
            return action != delete_action and bool(_get_project_resource_uris())

        should_put_project_team_members = rail.IfOperator(
            task_id='should_put_project_team_members',
            test=check_should_put_team_members,
            yes_task='put_project_team_members',
            no_task='update_task_details'
        )

        put_project_team_members = rail.RepliconServiceOperator(
            task_id='put_project_team_members',
            endpoint='/services/ProjectService1.svc/PutProjectTeamMemberAssignments',
            data=lambda: {
                'projectUri': rail.result('fetch_project_details')[0]['projectDetails']['uri'],
                'resourceUris': _get_project_resource_uris()
            }
        )

        def check_if_key_values_required():
            project = rail.result('fetch_project_details')[0]['projectDetails']
            if not project:
                return True

            ASSIGNMENT_TYPE_KEY = 'urn:replicon:project-key-value-key:project-team-member-assignment-type'
            ASSIGNMENT_TYPE_MANUAL = 'urn:replicon:project-team-member-assignment-type:manually-assign-task'
            for key_value in project['keyValues']:
                key = key_value['keyUri']
                value = key_value['value'].get('uri')
                if key == ASSIGNMENT_TYPE_KEY and value == ASSIGNMENT_TYPE_MANUAL:
                    return True
            return False

        should_assign_key_values = rail.IfOperator(
            task_id='should_assign_key_values',
            test=check_if_key_values_required,
            yes_task='assign_team_automatically',
            no_task='should_sync_supervisor_oef'
        )


        assign_team_automatically = rail.RepliconServiceOperator(
            task_id='assign_team_automatically',
            endpoint='/services/ProjectService1.svc/PutKeyValueForProject',
            data=lambda: {
                'projectUri': rail.result('sync_project_and_task')['uri'],
                'keyValue': {
                    'keyUri': 'urn:replicon:project-key-value-key:project-team-member-assignment-type',
                    'value': {
                        'uri': 'urn:replicon:project-team-member-assignment-type:automatically-assign-task'
                    }
                }
            }
        )


        should_sync_supervisor_oef = rail.IfOperator(
            task_id='should_sync_supervisor_oef',
            test="{{ result('managers_and_comanagers').SUPERVISOR | is_truthy }}",
            yes_task='sync_supervisor_oef',
            no_task='should_sync_principal_oef'
        )


        sync_supervisor_oef = rail.RepliconServiceOperator(
            task_id='sync_supervisor_oef',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda dag_run: request_payload.get_update_oef_param(dag_run, 'SUPERVISOR')
        )


        should_sync_principal_oef = rail.IfOperator(
            task_id='should_sync_principal_oef',
            test="{{ result('managers_and_comanagers').PRINCIPAL | is_truthy }}",
            yes_task='sync_principal_oef',
            no_task='should_assign_comanagers'
        )


        sync_principal_oef = rail.RepliconServiceOperator(
            task_id='sync_principal_oef',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda dag_run: request_payload.get_update_oef_param(dag_run, 'PRINCIPAL')
        )


        def get_comanagers(dag_run):
            comanagers = list(set([dag_run.conf['Supervisor'], dag_run.conf['Principal']]))
            if not comanagers[0] and len(comanagers) == 1:
                return True
            enabled_comanagers = list(filter(
                lambda x: x['isEnabled'] and x['loginName'] in comanagers,
                rail.result('fetch_replicon_users')
            ))
            return list(map(lambda x: x.get('uri', ''), enabled_comanagers))
        should_assign_comanagers = rail.IfOperator(
            task_id='should_assign_comanagers',
            test=get_comanagers,
            yes_task='should_assign_supervisor_permission',
            no_task='check_exceptions'
        )

        should_assign_supervisor_permission = rail.IfOperator(
            task_id='should_assign_supervisor_permission',
            test=lambda dag_run: dag_run.conf['Supervisor'] != dag_run.conf['ProjMgr'] \
                and test_if_assign_permission_required(dag_run.conf['Supervisor']),
            yes_task='assign_supervisor_permission',
            no_task='should_assign_principal_permission'
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=lambda dag_run: {
                'userUri': next(x for x in rail.result('fetch_replicon_users') \
                    if x['loginName'] == dag_run.conf['Supervisor'])['uri'],
                'permissionSetUri': rail.result('manager_permissions')[0]
            }
        )


        should_assign_principal_permission = rail.IfOperator(
            task_id='should_assign_principal_permission',
            test=lambda dag_run: dag_run.conf['Principal'] != dag_run.conf['ProjMgr'] \
                and test_if_assign_permission_required(dag_run.conf['Principal']),
            yes_task='assign_principal_permission',
            no_task='assign_comanagers'
        )

        assign_principal_permission = rail.RepliconServiceOperator(
            task_id='assign_principal_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=lambda dag_run: {
                'userUri': next(x for x in rail.result('fetch_replicon_users') \
                    if x['loginName'] == dag_run.conf['Principal'])['uri'],
                'permissionSetUri': rail.result('manager_permissions')[0]
            }
        )

        assign_comanagers = rail.RepliconServiceOperator(
            task_id='assign_comanagers',
            endpoint='/services/ProjectService1.svc/PutExplicitSharingAssignments',
            data=lambda dag_run: {
                'projectUri': rail.result('sync_project_and_task')['uri'],
                'sharedUris': get_comanagers(dag_run) if dag_run.conf['Supervisor'] or dag_run.conf['Principal'] else []
            }
        )


        check_exceptions = rail.PythonOperator(
            task_id='check_exceptions',
            python_callable=python_callable_method.check_exceptions
        )

        if_manager_not_found = rail.IfOperator(
            task_id='if_manager_not_found',
            test=lambda: bool(rail.result('check_exceptions').get('MANAGER')),
            yes_task='write_manager_exception',
            no_task='if_supervisor_not_found'
        )
        write_manager_exception = rail.WriteLogOperator(
            task_id='write_manager_exception',
            message='Exceptions',
            severity='Error/Exception',
            properties={
                'code': '{{ result("check_exceptions").MANAGER.code }}',
                'action': '{{ result("check_exceptions").MANAGER.action }}',
                'status': '{{ result("check_exceptions").MANAGER.status }}',
                'reason': '{{ result("check_exceptions").MANAGER.reason }}'
            }
        )

        if_supervisor_not_found = rail.IfOperator(
            task_id='if_supervisor_not_found',
            test=lambda: bool(rail.result('check_exceptions').get('SUPERVISOR')),
            yes_task='write_supervisor_exception',
            no_task='if_principal_not_found'
        )
        write_supervisor_exception = rail.WriteLogOperator(
            task_id='write_supervisor_exception',
            message='Exceptions',
            severity='Error/Exception',
            properties={
                'code': '{{ result("check_exceptions").SUPERVISOR.code }}',
                'action': '{{ result("check_exceptions").SUPERVISOR.action }}',
                'status': '{{ result("check_exceptions").SUPERVISOR.status }}',
                'reason': '{{ result("check_exceptions").SUPERVISOR.reason }}'
            }
        )

        if_principal_not_found = rail.IfOperator(
            task_id='if_principal_not_found',
            test=lambda: bool(rail.result('check_exceptions').get('PRINCIPAL')),
            yes_task='write_principal_exception',
            no_task='catch_error'
        )

        write_principal_exception = rail.WriteLogOperator(
            task_id='write_principal_exception',
            message='Exceptions',
            severity='Error/Exception',
            properties={
                'code': '{{ result("check_exceptions").PRINCIPAL.code }}',
                'action': '{{ result("check_exceptions").PRINCIPAL.action }}',
                'status': '{{ result("check_exceptions").PRINCIPAL.status }}',
                'reason': '{{ result("check_exceptions").PRINCIPAL.reason }}'
            }
        )


        def get_error_body():
            err = rail.render_template('{{ get_error_message() }}')
            if type(err) == str:
                status = 'Error'
                reason = err
            else:
                status = err['response']['status_code'] \
                    if err.get('response') else 'Error'
                reason = err['response']['json']['error']['reason'] \
                    if err.get('response') else err

            return {
                'code': rail.render_template('{{ dag_run.conf.WBS1 }}'),
                'action': rail.render_template('{{ dag_run.conf.Action }}'),
                'status': status,
                'reason': reason
            }
        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=get_error_body
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        def _lc_oef_uri(suffix):
            oef = request_payload.find_timesheet_lc_oef(
                rail.result(f'fetch_time_entry_oefs{suffix}'),
                config.timesheet_field_oef_name_for_lc
            )
            return (oef or {}).get('uri')

        def build_timesheet_lc_branch(suffix, downstream):
            should = rail.IfOperator(
                task_id=f'should_sync_timesheet_lc{suffix}',
                test=_should_sync_timesheet_lc,
                yes_task=f'fetch_time_entry_oefs{suffix}',
                no_task=downstream.task_id
            )

            fetch_time_entry_oefs = rail.RepliconServiceOperator(
                task_id=f'fetch_time_entry_oefs{suffix}',
                endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
                data=lambda: { 'bindingContextUri': 'urn:replicon:object-type:time-entry' }
            )

            if_lc_oef_exists = rail.IfOperator(
                task_id=f'if_timesheet_lc_oef_exists{suffix}',
                test=lambda: bool(_lc_oef_uri(suffix)),
                yes_task=f'get_existing_lc_tags{suffix}',
                no_task=f'log_timesheet_lc_oef_missing{suffix}'
            )

            log_lc_oef_missing = rail.WriteLogOperator(
                task_id=f'log_timesheet_lc_oef_missing{suffix}',
                message='Exceptions',
                severity='Error/Exception',
                properties={
                    'code': '{{ dag_run.conf.WBS1 }}',
                    'action': '{{ dag_run.conf.Action }}',
                    'status': 'Exception',
                    'reason': f"Timesheet labor code OEF '{config.timesheet_field_oef_name_for_lc}' "
                              "does not exist on the time-entry binding"
                }
            )

            get_existing_lc_tags = rail.RepliconServicePageOperator(
                task_id=f'get_existing_lc_tags{suffix}',
                endpoint='/services/ObjectExtensionTagListService1.svc/GetData',
                data=lambda: request_payload.build_lc_tag_list_data(_lc_oef_uri(suffix)),
                page_handler=request_payload.page_handler,
                all_result_data_handler=request_payload.parse_lc_tag_details
            )

            get_existing_lc_assignments = rail.RepliconServiceOperator(
                task_id=f'get_existing_lc_assignments{suffix}',
                endpoint='/services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/GetPageOfProjectDependentTimeEntryObjectExtensionTags',
                data=lambda dag_run: request_payload.build_existing_lc_assignments_data(
                    dag_run.conf['WBS1'], config.timesheet_field_oef_name_for_lc
                )
            )

            prepare_lc_tag_modifications = rail.PythonOperator(
                task_id=f'prepare_lc_tag_modifications{suffix}',
                python_callable=request_payload.build_apply_lc_tag_modifications(
                    f'get_existing_lc_tags{suffix}',
                    f'get_existing_lc_assignments{suffix}',
                    config.timesheet_field_oef_name_for_lc
                )
            )

            apply_lc_tag_modifications = rail.RepliconServiceOperator(
                task_id=f'apply_lc_tag_modifications{suffix}',
                endpoint='/services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags',
                data=lambda: rail.result(f'prepare_lc_tag_modifications{suffix}')
            )

            should >> rail.Label('Yes') >> fetch_time_entry_oefs >> if_lc_oef_exists
            should >> rail.Label('No') >> downstream
            if_lc_oef_exists >> rail.Label('Yes') >> get_existing_lc_tags >> get_existing_lc_assignments \
                >> prepare_lc_tag_modifications >> apply_lc_tag_modifications >> downstream
            if_lc_oef_exists >> rail.Label('No') >> log_lc_oef_missing >> downstream

            return should

        timesheet_lc_project_branch = build_timesheet_lc_branch('_project', should_assign_key_values)
        timesheet_lc_phasetask_branch = build_timesheet_lc_branch('_phasetask', should_update_parent_time_entry)

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> fetch_project_details >> is_labor_code_sync_enabled

        is_labor_code_sync_enabled >> rail.Label('Yes') >> fetch_labor_codes >> if_project_exists
        is_labor_code_sync_enabled >> rail.Label('No') >> if_project_exists

        if_project_exists >> rail.Label('Yes') >> fetch_task_details >> is_phase_or_task
        if_project_exists >> rail.Label('No') >> is_phase_or_task

        is_phase_or_task >> rail.Label('No') >> should_sync_client
        is_phase_or_task >> rail.Label('Yes') >> should_delete_task

        should_delete_task >> rail.Label('No') >> is_parent_present
        should_delete_task >> rail.Label('Yes') >> has_time_entry

        has_time_entry >> rail.Label('No') >> delete_task >> should_update_parent_time_entry
        has_time_entry >> rail.Label('Yes') >> disable_task_or_subtask >> write_disable_task_exception >> catch_error

        is_parent_present >> rail.Label('Yes') >> is_lc_project_resource_enabled
        is_parent_present >> rail.Label('No') >> write_parent_exception >> catch_error

        is_lc_project_resource_enabled >> rail.Label('Yes') >> fetch_vp_team_members_lc >> is_employee_resources_present_lc
        is_lc_project_resource_enabled >> rail.Label('No') >> should_put_project_team_members
        is_employee_resources_present_lc >> rail.Label('Yes') >> fetch_replicon_project_resources_lc >> should_put_project_team_members
        is_employee_resources_present_lc >> rail.Label('No') >> should_put_project_team_members

        should_put_project_team_members >> rail.Label('Yes') >> put_project_team_members >> update_task_details
        should_put_project_team_members >> rail.Label('No') >> update_task_details

        update_task_details >> should_sync_labor_codes

        should_sync_labor_codes >> rail.Label('Yes') >> prepare_labor_code_tasks >> sync_labor_code_tasks >> timesheet_lc_phasetask_branch
        should_sync_labor_codes >> rail.Label('No') >> timesheet_lc_phasetask_branch

        should_update_parent_time_entry >> rail.Label('Yes') >> modify_parent_time_entry_params >> update_parent_time_entry >> catch_error
        should_update_parent_time_entry >> rail.Label('No') >> catch_error

        should_sync_client >> rail.Label('Yes') >> sync_client >> fetch_project_oefs
        should_sync_client >> rail.Label('No') >> fetch_project_oefs

        fetch_project_oefs >> fetch_replicon_users >> managers_and_comanagers >> is_manager_permission_required

        is_manager_permission_required >> rail.Label('Yes') >> manager_permissions >> should_assign_manager_permission
        is_manager_permission_required >> rail.Label('No') >> should_assign_manager_permission

        should_assign_manager_permission >> rail.Label('Yes') >> assign_manager_permission >> is_project_resource_enabled
        should_assign_manager_permission >> rail.Label('No') >> is_project_resource_enabled

        is_project_resource_enabled >> rail.Label('Yes') >> fetch_vp_team_members >> is_employee_resources_present
        is_project_resource_enabled >> rail.Label('No') >> sync_project_and_task
        is_employee_resources_present >> rail.Label('Yes') >> fetch_replicon_project_resources >> sync_project_and_task
        is_employee_resources_present >> rail.Label('No') >> sync_project_and_task

        sync_project_and_task >> timesheet_lc_project_branch

        should_assign_key_values >> rail.Label('Yes') >> assign_team_automatically >> should_sync_supervisor_oef
        should_assign_key_values >> rail.Label('No') >> should_sync_supervisor_oef

        should_sync_supervisor_oef >> rail.Label('Yes') >> sync_supervisor_oef >> should_sync_principal_oef
        should_sync_supervisor_oef >> rail.Label('No') >> should_sync_principal_oef 

        should_sync_principal_oef >> rail.Label('Yes') >> sync_principal_oef >> should_assign_comanagers
        should_sync_principal_oef >> rail.Label('No') >> should_assign_comanagers

        should_assign_comanagers >> rail.Label('No') >> check_exceptions
        should_assign_comanagers >> rail.Label('Yes') >> should_assign_supervisor_permission

        should_assign_supervisor_permission >> rail.Label('Yes') >> assign_supervisor_permission >> should_assign_principal_permission
        should_assign_supervisor_permission >> rail.Label('No') >> should_assign_principal_permission

        should_assign_principal_permission >> rail.Label('Yes') >> assign_principal_permission >> assign_comanagers
        should_assign_principal_permission >> rail.Label('No') >> assign_comanagers >> check_exceptions >> if_manager_not_found
        
        if_manager_not_found >> rail.Label('No') >> if_supervisor_not_found
        if_manager_not_found >> rail.Label('Yes') >> write_manager_exception >> if_supervisor_not_found

        if_supervisor_not_found >> rail.Label('No') >> if_principal_not_found
        if_supervisor_not_found >> rail.Label('Yes') >> write_supervisor_exception >> if_principal_not_found

        if_principal_not_found >> rail.Label('Yes') >> write_principal_exception >> catch_error
        if_principal_not_found >> rail.Label('No') >> catch_error >> log_to_sumo


    return dag


rail.for_each_instance(create_child_dag)

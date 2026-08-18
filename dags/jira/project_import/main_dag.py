import itertools
import json
import logging
from datetime import datetime, timedelta
import rail
from airflow.models import Variable

null = None
# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/jira/main_dag/config.py

# pylint: disable=too-many-statements
def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description=f'Jira {config.region} Project Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        multi_tenant=True
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_lastsync_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_lastsync_time',
            end_task='should_log_history',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_lastsync_time = rail.GetLastSyncTimeOperator(
            task_id='get_lastsync_time',
            workflow_name=config.workflow,
            date_format='%Y-%m-%d %H:%M',
            initial_sync_time=lambda: (
                datetime(year=1970, month=1, day=1)).strftime('%Y-%m-%d %H:%M:%S'),
            provider=config.provider
        )

        def get_jira_project_details(response):
            projects = []
            for project in response:
                projects.append({
                    "project_key": project.get('key'),
                    "project_id": project.get('id'),
                    "project_name": project.get('name'),
                    "project_description": project.get('description'),
                    "project_leader": project['lead'].get('displayName'),
                    "project_leader_acc_id": project['lead'].get('accountId')
                })
            return projects

        search_jira_project_list = rail.JiraAPIOperator(
            task_id='search_jira_project_list',
            request_method='GET',
            endpoint="/rest/api/3/project/search",
            query_params={"expand": "description,lead"},
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            data_handler=get_jira_project_details
        )

        if_jira_project_present = rail.IfOperator(
            task_id='if_jira_project_present',
            test=lambda: len(rail.result('search_jira_project_list')) > 0,
            yes_task='is_issue_as_project_mode',
            no_task='delete_this_dagrun'
        )

        def _is_feature_flag_on():
            """Returns True when wbsSyncSetting is present in dag_run.conf (feature flag enabled)."""
            try:
                conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
                conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
                wbs = (conf.get('customSettings') or {}).get('wbsSyncSetting') or {}
                logging.info(f"WBS Sync Setting: {wbs}")
                return bool(wbs)
            except Exception as exc:
                logging.warning(f"_is_feature_flag_on: failed to parse dag_run.conf, defaulting to False: {exc}")
                return False

        def _is_mode_b():
            """Returns True when feature flag is on AND projectSyncSetting is not ['Project'] (issue-as-project mode)."""
            if not _is_feature_flag_on():
                return False
            try:
                conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
                conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
                wbs = (conf.get('customSettings') or {}).get('wbsSyncSetting') or {}
                project_sync = (wbs.get('mappings') or {}).get('projectSyncSetting', ['Project'])
                return 'Project' not in project_sync
            except Exception as exc:
                logging.warning(f"_is_mode_b: failed to parse dag_run.conf, defaulting to False: {exc}")
                return False

        is_issue_as_project_mode = rail.IfOperator(
            task_id='is_issue_as_project_mode',
            test=_is_mode_b,
            yes_task='search_jira_issue_projects',
            no_task='project_list_data'
        )

        def _build_mode_b_jql():
            conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
            conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
            wbs = (conf.get('customSettings') or {}).get('wbsSyncSetting') or {}
            hierarchy_levels = wbs.get('hierarchyLevels', [])
            project_level = next(
                (lvl for lvl in hierarchy_levels
                 if lvl.get('repliconLevel') == 'project' and lvl.get('enabled')), None
            )
            if not project_level:
                return 'issueKey is EMPTY'
            name = project_level['name']
            jira_hierarchy_level = project_level.get('hierarchyLevel')
            if jira_hierarchy_level == -1:
                type_clause = 'issueType in subTaskIssueTypes()'
            else:
                type_clause = f'issuetype = "{name}"'
            return f'{type_clause} ORDER BY updated ASC'

        def _parse_mode_b_issues(response):
            if not response:
                return []
            return [
                {
                    'project_key': issue['fields']['project']['key'],
                    'project_id': issue['key'],
                    'project_name': issue['fields']['summary'],
                    'project_leader': (issue['fields'].get('assignee') or {}).get('displayName'),
                    'project_leader_acc_id': (issue['fields'].get('assignee') or {}).get('accountId'),
                }
                for issue in response
                if isinstance(issue, dict)
            ]

        search_jira_issue_projects = rail.JiraAPIOperator(
            task_id='search_jira_issue_projects',
            request_method='GET',
            endpoint='/rest/api/3/search/jql',
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            query_params=lambda: {
                'jql': _build_mode_b_jql(),
                'fields': 'key,summary,assignee,project'
            },
            data_handler=_parse_mode_b_issues
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return null

        def all_result_data_handler(result):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], result))))
            return list(map(lambda row: {
                'uri': row['cells'][0]['uri'],
                'name': row['cells'][0]['textValue'],
                'code': row['cells'][3].get('textValue') if len(row['cells']) > 3 else None,
                'leader': row['cells'][2]
            }, flaten_rows))

        project_list_data = rail.RepliconServicePageOperator(
            task_id='project_list_data',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/ProjectListService1.svc/GetData',
            data={
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:name",
                    "urn:replicon:project-list-column:project-leader",
                    "urn:replicon:project-list-column:code"
                ],
            },
            page_handler=page_handler,
            all_result_data_handler=all_result_data_handler
        )

        def get_new_jira_projects():
            if not _is_mode_b():
                # Mode A: match against Replicon project names (existing behavior)
                project_set = {project['name'] for project in rail.result('project_list_data')}
                return [item for item in rail.result('search_jira_project_list')
                        if item['project_name'] not in project_set]
            # Mode B: match Jira issue keys against Replicon project codes
            code_set = {p['code'] for p in rail.result('project_list_data') if p.get('code')}
            return [item for item in (rail.result('search_jira_issue_projects') or [])
                    if item['project_id'] not in code_set]

        new_jira_projects = rail.PythonOperator(
            task_id="new_jira_projects",
            python_callable=get_new_jira_projects
        )

        has_jira_projects = rail.IfOperator(
            task_id="has_jira_projects",
            test=lambda: len(rail.result('new_jira_projects')) > 0,
            yes_task='get_my_actual_useridentity',
            no_task='should_trigger_hierarchy_sync'
        )

        get_my_actual_useridentity = rail.RepliconServiceOperator(
            task_id='get_my_actual_useridentity',
            endpoint='/services/UserAccessControlService1.svc/GetMyActualUserIdentity',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        def validate_for_polaris_permissions(response):
            view_psa_v2_permission = 'urn:replicon:psa-action:view-psa-v2'
            return any(filter(lambda item: item['permissionActionUri'] == view_psa_v2_permission, response))

        is_polaris_permissions_present = rail.RepliconServiceOperator(
            task_id='is_polaris_permissions_present',
            endpoint='/services/UserAccessControlService1.svc/GetEffectivePermissions',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={'userUri': "{{ result('get_my_actual_useridentity').uri }}"},
            data_handler=validate_for_polaris_permissions
        )

        skip_leader_sync_for_mode_b = rail.IfOperator(
            task_id='skip_leader_sync_for_mode_b',
            test=lambda: not _is_feature_flag_on() or _is_mode_b(),
            yes_task='trigger_project_import_child_dag',
            no_task='get_unique_project_leaders_for_sync'
        )

        trigger_project_import_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_project_import_child_dag',
            items=lambda: rail.result('new_jira_projects'),
            retries=0,
            trigger_rule='one_success',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.project_import_child_dag_id,
            conf=lambda item, dag_run: {
                **{
                    'project_key': item['project_key'],
                    'project_id': item['project_id'],
                    'project_name': item['project_name'],
                    'project_leader': None if _is_mode_b() else item['project_leader'],
                    'project_leader_acc_id': None if _is_mode_b() else item['project_leader_acc_id'],
                    'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                    'jira_conn_id': dag_run.conf['jira_conn_id'],
                    'is_polaris_permissions_present': rail.result('is_polaris_permissions_present')
                },
                **{k: v for k, v in dag_run.conf.items()
                   if k not in ('_ancestry', '_ecid', '_replication_position')}
            }
        )

        wait_for_project_import_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_project_import_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_project_import_child_dag") }}'
        )

        def _get_unique_project_leader_account_ids():
            """Collect unique project leader accountIds from new projects."""
            new_projects = rail.result('new_jira_projects') or []
            seen = set()
            for project in new_projects:
                acc_id = project.get('project_leader_acc_id')
                if acc_id and acc_id not in seen:
                    seen.add(acc_id)
            logging.info(f"Found {len(seen)} unique project leader account ID(s) for user pre-sync")
            return list(seen)

        get_unique_project_leaders_for_sync = rail.PythonOperator(
            task_id='get_unique_project_leaders_for_sync',
            python_callable=_get_unique_project_leader_account_ids
        )

        def _build_leader_account_id_params():
            """Build query params for Jira /user/bulk.
            Passing accountId as a list in a dict causes requests to encode each value
            as a separate param: ?accountId=id1&accountId=id2&...
            """
            account_ids = rail.result('get_unique_project_leaders_for_sync') or []
            return {'accountId': account_ids}

        def _parse_jira_bulk_user_response(response):
            """
            JiraAPIOperator extracts the 'values' array internally (_process_response does
            all_results.extend(data['values'])), so data_handler receives a flat list of
            user objects: [{accountId, emailAddress, displayName, active, timeZone, ...}, ...]
            """
            if not isinstance(response, list):
                logging.warning(f"Unexpected bulk user response type: {type(response)}")
                return []
            logging.info(f"Fetched {len(response)} user(s) from Jira bulk endpoint")
            return [
                {
                    'accountId': user.get('accountId', ''),
                    'emailAddress': user.get('emailAddress', ''),
                    'displayName': user.get('displayName', ''),
                    'active': user.get('active', True),
                    'timeZone': user.get('timeZone') or None,
                    'accountType': user.get('accountType', 'atlassian'),
                }
                for user in response
                if isinstance(user, dict)
            ]

        fetch_jira_details_for_leaders = rail.JiraAPIOperator(
            task_id='fetch_jira_details_for_leaders',
            request_method='GET',
            endpoint='/rest/api/3/user/bulk',
            query_params=_build_leader_account_id_params,
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            data_handler=_parse_jira_bulk_user_response
        )

        def _extract_existing_user_emails(response):
            """Return a list of lowercase email addresses of all existing Replicon users."""
            if not response or not hasattr(response, 'json'):
                return []
            try:
                rows = response.json()['d']['rows']
                return [
                    row['cells'][0].get('textValue', '').lower()
                    for row in rows
                    if row.get('cells') and row['cells'][0].get('textValue')
                ]
            except (KeyError, TypeError):
                return []

        get_replicon_users_for_dedup = rail.RepliconServiceOperator(
            task_id='get_replicon_users_for_dedup',
            endpoint='/services/UserListService1.svc/GetData',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                'page': '1',
                'pagesize': '10000',
                'columnUris': ['urn:replicon:user-list-column:email-address'],
                'sort': []
            },
            response_filter=_extract_existing_user_emails
        )

        def _filter_leaders_needing_creation():
            """Keep only leaders whose email is not yet in Replicon."""
            jira_users = rail.result('fetch_jira_details_for_leaders') or []
            existing_emails = set(rail.result('get_replicon_users_for_dedup') or [])
            new_leaders = [
                user for user in jira_users
                if user.get('emailAddress')
                and user['emailAddress'].lower() not in existing_emails
            ]
            logging.info(
                f"{len(jira_users)} unique leader(s) from Jira: "
                f"{len(new_leaders)} need creation, "
                f"{len(jira_users) - len(new_leaders)} already exist in Replicon"
            )
            return new_leaders

        filter_leaders_needing_creation = rail.PythonOperator(
            task_id='filter_leaders_needing_creation',
            python_callable=_filter_leaders_needing_creation
        )

        has_unique_leaders_to_sync = rail.IfOperator(
            task_id='has_unique_leaders_to_sync',
            test=lambda: len(rail.result('filter_leaders_needing_creation') or []) > 0,
            yes_task='trigger_user_creates_for_projects',
            no_task='trigger_project_import_child_dag'
        )

        trigger_user_creates_for_projects = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_creates_for_projects',
            items=lambda: rail.result('filter_leaders_needing_creation'),
            trigger_dag_id=config.create_user_child_dag_id,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                'user': {
                    'accountId': item.get('accountId', ''),
                    'emailAddress': item.get('emailAddress', ''),
                    'displayName': item.get('displayName', ''),
                    'accountType': item.get('accountType', 'atlassian'),
                    'active': item.get('active', True),
                    'timeZone': item.get('timeZone') or None,
                },
                'is_admin': True,
                'replicon_conn_id': dag_run.conf.get('replicon_conn_id', ''),
                'jira_conn_id': dag_run.conf.get('jira_conn_id', ''),
            }
        )

        wait_for_user_creates_for_projects = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_creates_for_projects',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_user_creates_for_projects") }}'
        )

        gather_project_import_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_project_import_error',
            dag_runs="{{ result('trigger_project_import_child_dag') }}",
            dagrun_task_id='catch_project_import_error',
            flatten=True
        )

        is_project_import_error = rail.IfOperator(
            task_id='is_project_import_error',
            # pylint: disable=line-too-long
            test="{{ (get_task_state('gather_project_import_error') == 'success' and result('gather_project_import_error') | length > 0)}}",
            yes_task='fail_project_import_error',
            no_task='should_trigger_hierarchy_sync'
        )

        fail_project_import_error = rail.FailOperator(
            task_id='fail_project_import_error',
            message="{{ result('gather_project_import_error') | map_to_attr('error') | join('|') }}"
        )

        # Triggers hierarchy sync only when wbsSyncSetting includes task-level mappings (M1-M6).
        # Skips for project-only modes (M7, M8, M9, M10).
        def check_should_trigger_hierarchy_sync():
            return _is_feature_flag_on()

        should_trigger_hierarchy_sync = rail.IfOperator(
            task_id='should_trigger_hierarchy_sync',
            test=check_should_trigger_hierarchy_sync,
            trigger_rule='all_done',
            yes_task='trigger_hierarchy_sync_child_dag',
            no_task='should_log_history'
        )

        trigger_hierarchy_sync_child_dag = rail.TriggerDagRunOperator(
            task_id='trigger_hierarchy_sync_child_dag',
            trigger_dag_id=config.hierarchy_sync_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                **{k: v for k, v in dag_run.conf.items()
                   if k not in ('_ancestry', '_ecid', '_replication_position')},
                'new_project_keys': [
                    p['project_key'] for p in (rail.result('new_jira_projects') or [])
                ],
                'last_synctime': (
                    rail.result('get_lastsync_time').get('last_synctime')
                    if isinstance(rail.result('get_lastsync_time'), dict) else None
                ),
            }
        )

        wait_for_hierarchy_sync_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_hierarchy_sync_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_hierarchy_sync_child_dag") }}'
        )
        
        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not(get_task_state('new_jira_projects') == 'success' and result('new_jira_projects') | length == 0 and get_task_state('should_trigger_hierarchy_sync') == 'success' and not result('should_trigger_hierarchy_sync')) }}",
            trigger_rule='all_done',
            yes_task='log_dagrun_details_to_table',
            no_task='delete_this_dagrun'
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            trigger_rule='all_done',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ dag_run.conf.company_key }}',
            connector_name='jira',
            integration_type='project_import'
        )

        update_lastsync_time = rail.SetLastSyncTimeOperator(
            task_id='update_lastsync_time',
            trigger_rule='all_done',
            workflow_name=config.workflow,
            provider=config.provider,
            value_to_set='{{result("get_lastsync_time").current_time}}'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label('No') >> get_lastsync_time >> search_jira_project_list

        search_jira_project_list >> if_jira_project_present
        if_jira_project_present >> rail.Label('Yes') >> is_issue_as_project_mode
        is_issue_as_project_mode >> rail.Label('Yes') >> search_jira_issue_projects >> project_list_data
        is_issue_as_project_mode >> rail.Label('No') >> project_list_data
        project_list_data >> new_jira_projects >> has_jira_projects
        if_jira_project_present >> rail.Label('No') >> delete_this_dagrun

        has_jira_projects >> rail.Label('Yes') >> get_my_actual_useridentity >> is_polaris_permissions_present \
            >> skip_leader_sync_for_mode_b
        skip_leader_sync_for_mode_b >> rail.Label('Yes') >> trigger_project_import_child_dag
        skip_leader_sync_for_mode_b >> rail.Label('No') >> get_unique_project_leaders_for_sync >> fetch_jira_details_for_leaders \
            >> get_replicon_users_for_dedup >> filter_leaders_needing_creation >> has_unique_leaders_to_sync
        has_unique_leaders_to_sync >> rail.Label('Yes') >> trigger_user_creates_for_projects \
            >> wait_for_user_creates_for_projects >> trigger_project_import_child_dag
        has_unique_leaders_to_sync >> rail.Label('No') >> trigger_project_import_child_dag
        trigger_project_import_child_dag >> wait_for_project_import_child_dag \
            >> gather_project_import_error >> is_project_import_error

        has_jira_projects >> rail.Label('No') >> should_trigger_hierarchy_sync
        is_project_import_error >> rail.Label('Yes') >> fail_project_import_error >> should_log_history
        is_project_import_error >> rail.Label('No') >> should_trigger_hierarchy_sync

        # Hierarchy child DAG triggers only when wbsSyncSetting mapping config is present in dag_run.conf
        should_trigger_hierarchy_sync >> rail.Label('Yes') >> trigger_hierarchy_sync_child_dag \
            >> wait_for_hierarchy_sync_child_dag >> should_log_history
        should_trigger_hierarchy_sync >> rail.Label('No') >> should_log_history

        should_log_history >> rail.Label('Yes') >> log_dagrun_details_to_table >> update_lastsync_time
        should_log_history >> rail.Label('No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)

from datetime import datetime, timedelta
import rail
from airflow.models import Variable

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_jira_{config.region.replace('-', '_')}_create_user_{config.instance}",
        description=f'Jira {config.region} Create User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: (Variable.get(
                config.can_run_batch_task_var_name, default_var='true') or 'true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_lastsync_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_lastsync_time',
            end_task='should_log_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_lastsync_time = rail.GetLastSyncTimeOperator(
            task_id='get_lastsync_time',
            workflow_name=config.workflow,
            date_format='%Y-%m-%d %H:%M',
            initial_sync_time=lambda: (
                datetime(year=1970, month=1, day=1)).strftime('%Y-%m-%d %H:%M:%S'),
            provider=config.provider
        )

        def get_users_details(response):
            """
            Extract user details from Jira API response.
            """
            if not response:
                return []

            # Flatten list of lists into a single list of user dicts
            flat_response = [item for page in response for item in (page if isinstance(page, list) else [page])]

            users = []
            for item in flat_response:
                try:
                    if not item.get("emailAddress"):
                        continue
                    if item.get("accountType") != 'atlassian':
                        continue
                    
                    user = {
                        "accountId": item.get("accountId", ""),
                        "emailAddress": item.get("emailAddress", ""),
                        "displayName": item.get("displayName", ""),
                        "accountType": item.get("accountType", ""),
                        "active": item.get("active", True),
                        "timeZone": item.get("timeZone") or None,
                    }
                    users.append(user)
                except (KeyError, TypeError):
                    continue        
            return users
        
        fetch_all_jira_users = rail.JiraAPIOperator(
            task_id='fetch_all_jira_users',
            request_method='GET',
            endpoint='/rest/api/3/users/search',
            query_params={'query': '.', 'accountType': 'atlassian'},
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            data_handler=get_users_details
        )

        fetch_all_jira_roles = rail.JiraAPIOperator(
            task_id='fetch_all_jira_roles',
            request_method='GET',
            endpoint='/rest/api/3/role',
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}'
        )

        def resolve_role_ids_func():
            raw_roles = rail.result('fetch_all_jira_roles') or []
            # Flatten in case JiraAPIOperator returned paginated list-of-lists
            if raw_roles and isinstance(raw_roles[0], list):
                all_jira_roles = [role for page in raw_roles for role in page]
            else:
                all_jira_roles = raw_roles
            selected_roles = (rail.result('determine_sync_mode') or {}).get('sync_data', [])

            selected_role_names = set()
            for role_input in selected_roles:
                role_name = role_input if isinstance(role_input, str) else role_input.get('name', '')
                if role_name:
                    selected_role_names.add(role_name.lower())

            # The /rest/api/3/role response already embeds scope.project.id for project-scoped
            # roles. Extract project_id directly — no need to fetch projects separately.
            matched_roles = []
            for role in all_jira_roles:
                if not isinstance(role, dict):
                    continue
                role_name = role.get('name', '')
                if role_name.lower() not in selected_role_names:
                    continue
                scope = role.get('scope') or {}
                project_id = (scope.get('project') or {}).get('id')
                matched_roles.append({
                    'name': role_name,
                    'id': role.get('id'),
                    'project_id': project_id  # None for global roles
                })
            return matched_roles

        resolve_role_ids = rail.PythonOperator(
            task_id='resolve_role_ids',
            python_callable=resolve_role_ids_func
        )

        # ================================================================
        # ROLE-BASED: ForEach role endpoint → JiraAPIOperator
        # ================================================================

        def build_role_endpoints_func():
            """Build role API endpoints using project_id already embedded in role scope.
            Project-scoped roles → /rest/api/3/project/{project_id}/role/{role_id}
            Global roles (no scope) → /rest/api/3/role/{role_id}
            """
            matched_roles = rail.result('resolve_role_ids') or []
            if not matched_roles:
                return []
            endpoints = []
            for role_info in matched_roles:
                project_id = role_info.get('project_id')
                role_id = role_info.get('id')
                if project_id:
                    endpoints.append(f"/rest/api/3/project/{project_id}/role/{role_id}")
                else:
                    endpoints.append(f"/rest/api/3/role/{role_id}")
            return endpoints

        build_role_endpoints = rail.PythonOperator(
            task_id='build_role_endpoints',
            python_callable=build_role_endpoints_func
        )

        declare_role_actor_ids = rail.SetVariableOperator(
            task_id='declare_role_actor_ids',
            append=False,
            name='role_actor_ids_{{ run_id }}',
            value=[]
        )

        process_role_endpoints = rail.ForEachOperator(
            task_id='process_role_endpoints',
            items=lambda: rail.result('build_role_endpoints'),
            start_task='fetch_role_actors',
            end_task='role_actors_collected'
        )

        def extract_role_actor_ids(results):
            """Data handler: extract atlassian-user-role-actor accountIds."""
            return [
                actor['actorUser']['accountId']
                for item in results
                for actor in item.get('actors', [])
                if actor.get('type') == 'atlassian-user-role-actor'
                and actor.get('actorUser', {}).get('accountId')
            ]

        fetch_role_actors = rail.JiraAPIOperator(
            task_id='fetch_role_actors',
            request_method='GET',
            endpoint='{{ result("process_role_endpoints") }}',
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            data_handler=extract_role_actor_ids
        )

        add_role_actor_ids = rail.SetVariableOperator(
            task_id='add_role_actor_ids',
            append=True,
            name='role_actor_ids_{{ run_id }}',
            value=lambda: rail.result('fetch_role_actors')
        )

        role_actors_collected = rail.EmptyOperator(
            task_id='role_actors_collected'
        )

        get_role_actor_ids = rail.GetVariableOperator(
            task_id='get_role_actor_ids',
            name='role_actor_ids_{{ run_id }}'
        )

        def filter_role_users_func():
            """Filter fetched Jira users by accumulated role actor IDs."""
            accumulated = (rail.result('get_role_actor_ids') or {}).get('value', [])
            unique_ids = set()
            for id_list in accumulated:
                if isinstance(id_list, list):
                    unique_ids.update(id_list)
                elif id_list:
                    unique_ids.add(id_list)
            all_users = rail.result('fetch_all_jira_users') or []
            return [u for u in all_users if u.get('accountId') in unique_ids]

        filter_role_users = rail.PythonOperator(
            task_id='filter_role_users',
            python_callable=filter_role_users_func
        )

        # ================================================================
        # GROUP-BASED: ForEach group → JiraAPIOperator
        # ================================================================

        declare_group_member_ids = rail.SetVariableOperator(
            task_id='declare_group_member_ids',
            append=False,
            name='group_member_ids_{{ run_id }}',
            value=[]
        )

        process_groups = rail.ForEachOperator(
            task_id='process_groups',
            items=lambda: (rail.result('determine_sync_mode') or {}).get('sync_data', []),
            start_task='resolve_group',
            end_task='group_members_collected'
        )

        def extract_group_id(results):
            """Data handler: extract groupId from group lookup response."""
            return results[0].get('groupId', '') if results else ''

        resolve_group = rail.JiraAPIOperator(
            task_id='resolve_group',
            request_method='GET',
            endpoint='/rest/api/3/group',
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            query_params={'groupname': '{{ result("process_groups") }}'},
            data_handler=extract_group_id
        )

        def extract_member_account_ids(results):
            """Data handler: extract active atlassian account IDs from group members."""
            return [
                m.get('accountId') for m in results
                if m.get('accountType') == 'atlassian'
                and m.get('active', False)
                and m.get('accountId')
            ]

        fetch_group_members = rail.JiraAPIOperator(
            task_id='fetch_group_members',
            request_method='GET',
            endpoint='/rest/api/3/group/member',
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            query_params={
                'groupId': '{{ result("resolve_group") }}',
                'includeInactiveUsers': 'false'
            },
            data_handler=extract_member_account_ids
        )

        add_group_member_ids = rail.SetVariableOperator(
            task_id='add_group_member_ids',
            append=True,
            name='group_member_ids_{{ run_id }}',
            value=lambda: rail.result('fetch_group_members')
        )

        group_members_collected = rail.EmptyOperator(
            task_id='group_members_collected'
        )

        get_group_member_ids = rail.GetVariableOperator(
            task_id='get_group_member_ids',
            name='group_member_ids_{{ run_id }}'
        )

        def filter_group_users_func():
            """Filter fetched Jira users by accumulated group member IDs."""
            accumulated = (rail.result('get_group_member_ids') or {}).get('value', [])
            unique_ids = set()
            for id_list in accumulated:
                if isinstance(id_list, list):
                    unique_ids.update(id_list)
                elif id_list:
                    unique_ids.add(id_list)
            all_users = rail.result('fetch_all_jira_users') or []
            return [u for u in all_users if u.get('accountId') in unique_ids]

        filter_group_users = rail.PythonOperator(
            task_id='filter_group_users',
            python_callable=filter_group_users_func
        )

        # ================================================================

        def collect_jira_users_func():
            all_users = rail.result('fetch_all_jira_users') or []
            sync_mode = (rail.result('determine_sync_mode') or {}).get('sync_mode', 'all_users')

            if sync_mode == 'all_users':
                return all_users

            if sync_mode == 'role_based':
                return rail.result('filter_role_users') or []
            else:
                return rail.result('filter_group_users') or []

        def determine_sync_mode():
            import json
            
            def parse_list(raw):
                if not raw:
                    return []
                if isinstance(raw, list):
                    return [item for item in raw if item]
                if isinstance(raw, str):
                    raw = raw.strip()
                    if not raw or raw == '[]':
                        return []
                    try:
                        parsed = json.loads(raw)
                        return parsed if isinstance(parsed, list) else []
                    except (json.JSONDecodeError, ValueError):
                        return [item.strip() for item in raw.split(',') if item.strip()]
                return []
            
            selected_roles_raw = rail.render_template(
                '{{ (dag_run.conf.get("selectedRoles") or dag_run.conf.get("customSettings", {}).get("selectedRoles", [])) | tojson }}'
            )
            selected_groups_raw = rail.render_template(
                '{{ (dag_run.conf.get("selectedGroups") or dag_run.conf.get("customSettings", {}).get("selectedGroups", [])) | tojson }}'
            )
            
            selected_roles = parse_list(selected_roles_raw)
            selected_groups = parse_list(selected_groups_raw)

            if selected_roles:
                sync_mode = 'role_based'
                sync_data = selected_roles
            elif selected_groups:
                sync_mode = 'group_based'
                sync_data = selected_groups
            else:
                sync_mode = 'all_users'
                sync_data = []
            
            return {
                'sync_mode': sync_mode,
                'sync_data': sync_data
            }
            
        
        determine_sync_mode_task = rail.PythonOperator(
            task_id='determine_sync_mode',
            python_callable=determine_sync_mode
        )

        is_all_users_mode = rail.IfOperator(
            task_id='is_all_users_mode',
            test=lambda: (rail.result('determine_sync_mode') or {}).get('sync_mode') == 'all_users',
            yes_task='jira_users',
            no_task='is_role_based'
        )

        is_role_based = rail.IfOperator(
            task_id='is_role_based',
            test=lambda: (rail.result('determine_sync_mode') or {}).get('sync_mode') == 'role_based',
            yes_task='fetch_all_jira_roles',
            no_task='declare_group_member_ids'
        )

        jira_users = rail.PythonOperator(
            task_id='jira_users',
            python_callable=collect_jira_users_func,
            trigger_rule='none_failed_min_one_success'
        )

        def add_md5_to_jira_users():
            import hashlib
            users = rail.result('jira_users') or []
            result_list = []
            for user in users:
                display_name = (user.get('displayName') or '').strip()
                name_parts = display_name.split()
                firstname = name_parts[0] if name_parts else ''
                lastname = name_parts[-1] if len(name_parts) > 1 else firstname
                email = user.get('emailAddress', '')
                tz_raw = user.get('timeZone') or ''
                timezone = config.JIRA_TIMEZONE_MAP.get(tz_raw) if tz_raw else None
                active = user.get('active', True)
                if not isinstance(active, bool):
                    active = str(active).lower() not in ('false', '0', 'no', '')
                active_str = str(active).lower()
                fields = ','.join([email, firstname, lastname, timezone or '', active_str])
                result_list.append({
                    **user,
                    'firstname': firstname,
                    'lastname': lastname,
                    'timeZone': timezone,
                    'active': active,
                    'md5': hashlib.md5(fields.encode('utf-8')).hexdigest()
                })
            return result_list

        compute_user_md5 = rail.PythonOperator(
            task_id='compute_user_md5',
            python_callable=add_md5_to_jira_users
        )

        # ================================================================
        # S3 REFERENCE FILE LOGIC (segregated by sync type)
        # Fallback priority: all_users → role_based → group_based
        # ================================================================

        S3_KEY_MAP = {
            'all_users': config.s3_all_users_reference_key,
            'role_based': config.s3_role_based_reference_key,
            'group_based': config.s3_group_based_reference_key,
        }

        FALLBACK_ORDER = {
            'all_users': ['role_based', 'group_based'],
            'role_based': ['all_users', 'group_based'],
            'group_based': ['all_users', 'role_based'],
        }

        def resolve_s3_keys_func():
            sync_mode = (rail.result('determine_sync_mode') or {}).get('sync_mode', 'all_users')
            primary_key = S3_KEY_MAP[sync_mode]
            fallback_keys = [S3_KEY_MAP[m] for m in FALLBACK_ORDER[sync_mode]]
            return {
                'sync_mode': sync_mode,
                'primary_key': primary_key,
                'fallback_keys': fallback_keys
            }

        resolve_s3_keys = rail.PythonOperator(
            task_id='resolve_s3_keys',
            python_callable=resolve_s3_keys_func
        )

        search_reference_files_in_s3 = rail.S3ListKeysOperator(
            task_id='search_reference_files_in_s3',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket,
            prefix=config.s3_reference_prefix
        )

        def check_primary_file_exists():
            s3_keys = rail.result('resolve_s3_keys') or {}
            primary_key = s3_keys.get('primary_key', '')
            found_keys = rail.result('search_reference_files_in_s3') or []
            return primary_key in found_keys

        if_primary_file_exists = rail.IfOperator(
            task_id='if_primary_file_exists',
            test=check_primary_file_exists,
            yes_task='download_primary_file',
            no_task='check_fallback_file_exists'
        )

        download_primary_file = rail.S3DownloadFileOperator(
            task_id='download_primary_file',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket,
            key_name="{{ result('resolve_s3_keys').primary_key }}"
        )

        load_primary_data = rail.LoadCSVFileOperator(
            task_id='load_primary_data',
            document="{{ result('download_primary_file') }}"
        )

        def compute_user_delta():
            """Compute delta against primary (own type) reference file."""
            current_users = rail.result('compute_user_md5') or []
            reference_records = rail.load_all_records(
                rail.result('load_primary_data')
            )
            reference_md5_set = {r['md5'] for r in reference_records}
            return [u for u in current_users if u['md5'] not in reference_md5_set]

        get_users_to_process = rail.PythonOperator(
            task_id='get_users_to_process',
            python_callable=compute_user_delta,
            trigger_rule='none_failed_min_one_success'
        )

        # --- Fallback path: primary file doesn't exist ---

        def find_fallback_key():
            """Find the first available fallback file key."""
            s3_keys = rail.result('resolve_s3_keys') or {}
            fallback_keys = s3_keys.get('fallback_keys', [])
            found_keys = rail.result('search_reference_files_in_s3') or []
            for key in fallback_keys:
                if key in found_keys:
                    return key
            return ''

        check_fallback_file_exists = rail.PythonOperator(
            task_id='check_fallback_file_exists',
            python_callable=find_fallback_key
        )

        if_fallback_file_exists = rail.IfOperator(
            task_id='if_fallback_file_exists',
            test=lambda: bool(rail.result('check_fallback_file_exists')),
            yes_task='download_fallback_file',
            no_task='get_users_to_process_no_ref'
        )

        download_fallback_file = rail.S3DownloadFileOperator(
            task_id='download_fallback_file',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket,
            key_name="{{ result('check_fallback_file_exists') }}"
        )

        load_fallback_data = rail.LoadCSVFileOperator(
            task_id='load_fallback_data',
            document="{{ result('download_fallback_file') }}"
        )

        def compare_with_fallback_func():
            """Compare current users with fallback file.
            Returns: {'is_same': bool, 'delta': list}
            """
            current_users = rail.result('compute_user_md5') or []
            fallback_records = rail.load_all_records(
                rail.result('load_fallback_data')
            )
            fallback_md5_set = {r['md5'] for r in fallback_records}
            current_md5_set = {u['md5'] for u in current_users}
            is_same = (current_md5_set == fallback_md5_set)
            delta = [u for u in current_users if u['md5'] not in fallback_md5_set]
            return {'is_same': is_same, 'delta': delta}

        compare_with_fallback = rail.PythonOperator(
            task_id='compare_with_fallback',
            python_callable=compare_with_fallback_func
        )

        if_data_matches_fallback = rail.IfOperator(
            task_id='if_data_matches_fallback',
            test=lambda: (rail.result('compare_with_fallback') or {}).get('is_same', False),
            yes_task='write_reference_csv',
            no_task='get_users_to_process_from_fallback'
        )

        def get_delta_from_fallback():
            return (rail.result('compare_with_fallback') or {}).get('delta', [])

        get_users_to_process_from_fallback = rail.PythonOperator(
            task_id='get_users_to_process_from_fallback',
            python_callable=get_delta_from_fallback
        )

        # --- No reference file at all (neither primary nor fallback) ---

        def all_users_as_new():
            return rail.result('compute_user_md5') or []

        get_users_to_process_no_ref = rail.PythonOperator(
            task_id='get_users_to_process_no_ref',
            python_callable=all_users_as_new
        )

        # --- Merge users to process from all paths ---

        def collect_users_to_process_func():
            """Collect users to process from whichever path was taken."""
            primary_result = rail.result('get_users_to_process')
            fallback_delta = rail.result('get_users_to_process_from_fallback')
            no_ref_result = rail.result('get_users_to_process_no_ref')
            if primary_result:
                return primary_result
            if fallback_delta:
                return fallback_delta
            if no_ref_result:
                return no_ref_result
            return []

        collect_users_to_process = rail.PythonOperator(
            task_id='collect_users_to_process',
            python_callable=collect_users_to_process_func,
            trigger_rule='none_failed_min_one_success'
        )

        has_users_to_process = rail.IfOperator(
            task_id='has_users_to_process',
            test=lambda: len(rail.result('collect_users_to_process')) > 0,
            yes_task='trigger_create_user_child_dag',
            no_task='write_reference_csv'
        )

        def extract_account_id(results):
            if isinstance(results, list) and results:
                return results[0].get('accountId')
            if isinstance(results, dict):
                return results.get('accountId')
            return None

        get_integration_user = rail.JiraAPIOperator(
            task_id='get_integration_user',
            request_method='GET',
            endpoint='/rest/api/3/myself',
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            data_handler=extract_account_id
        )

        def build_child_dag_conf(item, dag_run):
            integration_account_id = rail.result('get_integration_user') or ''
            conf = {
                'user': item,
                'is_admin': item.get('accountId') == integration_account_id,
            }
            if hasattr(dag_run, 'conf') and isinstance(dag_run.conf, dict):
                for key, value in dag_run.conf.items():
                    if key not in ('_ancestry', '_ecid', '_replication_position', 'user', 'is_admin'):
                        conf[key] = value
            return conf

        trigger_create_user_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_create_user_child_dag',
            items=lambda: rail.result('collect_users_to_process'),
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_jira_{config.region.replace('-', '_')}_create_user_child_dag_{config.instance}",
            conf=build_child_dag_conf
        )

        wait_for_create_user_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_user_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_create_user_child_dag") }}'
        )

        gather_create_user_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_create_user_error',
            dag_runs="{{ result('trigger_create_user_child_dag') }}",
            dagrun_task_id='catch_create_user_error',
            flatten=True
        )

        def check_for_errors():
            """Check if there are any errors (non-None values)"""
            errors = rail.result('gather_create_user_error') or []
            actual_errors = [e for e in errors if e is not None]
            return len(actual_errors) > 0

        is_create_user_error = rail.IfOperator(
            task_id='is_create_user_error',
            test=check_for_errors,
            yes_task='fail_create_user_error',
            no_task='write_reference_csv'
        )

        fail_create_user_error = rail.FailOperator(
            task_id='fail_create_user_error',
            message="{{ result('gather_create_user_error') | reject('none') | map('string') | join(' | ') or 'Unknown error occurred in child DAGs' }}"
        )

        write_reference_csv = rail.WriteCSVFileOperator(
            task_id='write_reference_csv',
            source="{{ result('compute_user_md5') | to_json }}",
            header=['accountId', 'emailAddress', 'firstname', 'lastname', 'timeZone', 'active', 'md5'],
            row=['{{ item.accountId }}', '{{ item.emailAddress }}', '{{ item.firstname }}',
                 '{{ item.lastname }}', '{{ item.timeZone or "" }}', '{{ item.active }}', '{{ item.md5 }}'],
            trigger_rule='none_failed_min_one_success'
        )

        upload_reference_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_reference_to_s3',
            aws_conn_id=config.aws_conn_id,
            source="{{ result('write_reference_csv') }}",
            bucket_name=config.s3_bucket,
            key_name="{{ result('resolve_s3_keys').primary_key }}",
            replace=True
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not(get_task_state('jira_users') == 'success' and \
                result('jira_users') | length == 0 )}}",
            trigger_rule='all_done',
            yes_task='log_dagrun_details_to_table',
            no_task='delete_this_dagrun'
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ (dag_run.conf or {}).get("company_key", "") }}',
            connector_name='jira',
            integration_type='create_user'
        )

        update_lastsync_time = rail.SetLastSyncTimeOperator(
            task_id='update_lastsync_time',
            workflow_name=config.workflow,
            provider=config.provider,
            value_to_set='{{result("get_lastsync_time").current_time}}'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time >> determine_sync_mode_task >> fetch_all_jira_users >> is_all_users_mode

        is_all_users_mode >> rail.Label('Yes') >> jira_users

        is_all_users_mode >> rail.Label('No') >> is_role_based

        # Role-based: ForEach role endpoint
        is_role_based >> rail.Label('Yes') >> fetch_all_jira_roles >> resolve_role_ids \
            >> build_role_endpoints >> declare_role_actor_ids \
            >> process_role_endpoints >> fetch_role_actors >> add_role_actor_ids \
            >> role_actors_collected
        process_role_endpoints >> role_actors_collected \
            >> get_role_actor_ids >> filter_role_users >> jira_users

        # Group-based flow
        is_role_based >> rail.Label('No') >> declare_group_member_ids \
            >> process_groups >> resolve_group >> fetch_group_members \
            >> add_group_member_ids >> group_members_collected
        process_groups >> group_members_collected \
            >> get_group_member_ids >> filter_group_users >> jira_users

        jira_users >> compute_user_md5 >> get_integration_user
        get_integration_user >> resolve_s3_keys >> search_reference_files_in_s3 >> if_primary_file_exists

        # Primary file exists: normal delta
        if_primary_file_exists >> rail.Label(
            'Yes') >> download_primary_file >> load_primary_data >> get_users_to_process >> collect_users_to_process
        # Primary file missing: check fallback
        if_primary_file_exists >> rail.Label(
            'No') >> check_fallback_file_exists >> if_fallback_file_exists

        # Fallback file exists: compare
        if_fallback_file_exists >> rail.Label(
            'Yes') >> download_fallback_file >> load_fallback_data >> compare_with_fallback >> if_data_matches_fallback
        # No fallback either: all users are new
        if_fallback_file_exists >> rail.Label(
            'No') >> get_users_to_process_no_ref >> collect_users_to_process

        # Data matches fallback: skip child DAG, just write type-specific file
        if_data_matches_fallback >> rail.Label(
            'Yes') >> write_reference_csv
        # Data differs from fallback: process the delta
        if_data_matches_fallback >> rail.Label(
            'No') >> get_users_to_process_from_fallback >> collect_users_to_process

        collect_users_to_process >> has_users_to_process

        has_users_to_process >> rail.Label(
            'Yes') >> trigger_create_user_child_dag >> wait_for_create_user_child_dag \
            >> gather_create_user_error >> is_create_user_error
        is_create_user_error >> rail.Label(
            'Yes') >> fail_create_user_error >> should_log_history
        is_create_user_error >> rail.Label(
            'No') >> write_reference_csv
        has_users_to_process >> rail.Label(
            'No') >> write_reference_csv
        write_reference_csv >> upload_reference_to_s3 >> should_log_history

        should_log_history >> rail.Label(
            'Yes') >> log_dagrun_details_to_table >> update_lastsync_time
        should_log_history >> rail.Label(
            'No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
from datetime import timedelta
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from nrdc.user_import_v3.utils import custom_method, request_payload, python_callable_method

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.nrdc_updating_c3_c4_values,
        description=f'NRDC C3/C4/Delegate Profile Management {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='initialize_supervisors'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='initialize_supervisors',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        initialize_supervisors = rail.PythonOperator(
            task_id="initialize_supervisors",
            python_callable=custom_method.c3_c4_supervisors_loginname,
            op_args=[config.c3_c4_profile_supervisors_variable]
        )

        validate_input = rail.IfOperator(
            task_id='validate_input',
            test=lambda dag_run: bool(
                dag_run.conf.get('department') and
                dag_run.conf.get('emailaddress') and
                dag_run.conf.get('logonname')
            ),
            yes_task="extract_current_profiles",
            no_task="log_validation_error"
        )

        log_validation_error = rail.WriteLogOperator(
            task_id='log_validation_error',
            message="User not Updated - Required fields missing",
            severity="Error",
            properties={
                "user": "{{ dag_run.conf.logonname }}",
                "action": "Update",
                "status": "Error",
                "details": "User not Updated, login name/email or Employee ID or department or employee type not present",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        extract_current_profiles = rail.PythonOperator(
            task_id='extract_current_profiles',
            python_callable=lambda dag_run: custom_method.get_profile_list(dag_run)
        )

        analyze_profile_requirements_task = rail.PythonOperator(
            task_id='analyze_profile_requirements',
            python_callable=lambda dag_run: custom_method.analyze_profile_requirements(
                dag_run,
                rail.result('extract_current_profiles')
            )
        )

        check_disable_needed = rail.IfOperator(
            task_id='check_disable_needed',
            test=lambda: len(rail.result('analyze_profile_requirements')['actions']['disable']) > 0,
            yes_task="execute_profile_disables",
            no_task="check_update_needed"
        )

        execute_profile_disables = rail.TriggerDagRunForEachItemOperator(
            task_id='execute_profile_disables',
            retries=0,
            items=lambda: rail.result('analyze_profile_requirements')['actions']['disable'],
            trigger_dag_id=config.nrdc_updaterehiredisableuserbasicprofile,
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run, item: request_payload.create_disable_profile_payload(
                dag_run,
                item['profile_data'],
                item['profile_type'],
                item['disabled_suffix'],
                item['config']
            )
        )

        wait_for_profile_disables = rail.WaitForDagRunsSensor(
            task_id='wait_for_profile_disables',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("execute_profile_disables") }}'
        )

        check_update_needed = rail.IfOperator(
            task_id='check_update_needed',
            test=lambda: len(rail.result('analyze_profile_requirements')['actions']['update']) > 0,
            yes_task="execute_profile_updates",
            no_task="check_create_needed"
        )

        execute_profile_updates = rail.TriggerDagRunForEachItemOperator(
            task_id='execute_profile_updates',
            retries=0,
            items=lambda: rail.result('analyze_profile_requirements')['actions']['update'],
            trigger_dag_id=config.nrdc_updaterehiredisableuserbasicprofile,
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run, item: request_payload.create_update_profile_payload(
                dag_run,
                item['profile_data'],
                item['profile_type'],
                item['is_primary'],
                item['config'],
                item['existing_primary'],
                next((p['uri'] for p in rail.result('analyze_profile_requirements')['existing_profiles']
                      if p['type'] == rail.result('analyze_profile_requirements')['primary_profile'] or
                      (p['type'] == 'Lobbying Timesheet' and rail.result('analyze_profile_requirements')['primary_profile'] == 'C3')), None)
            )
        )

        wait_for_profile_updates = rail.WaitForDagRunsSensor(
            task_id='wait_for_profile_updates',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("execute_profile_updates") }}'
        )

        check_create_needed = rail.IfOperator(
            task_id='check_create_needed',
            test=lambda: len(rail.result('analyze_profile_requirements')['actions']['create']) > 0,
            yes_task="for_each_create_entry",
            no_task="check_substitute_needed"
        )

        for_each_create_entry = rail.ForEachOperator(
            task_id='for_each_create_entry',
            items=lambda: rail.result('analyze_profile_requirements')['actions']['create'],
            start_task="get_user_loginname",
            end_task="for_each_create_entry_end"
        )

        get_user_loginname = rail.PythonOperator(
            task_id='get_user_loginname',
            python_callable=lambda dag_run: dag_run.conf['logonname'].split('@')[0] +  rail.result("for_each_create_entry")['config']['suffix']
        )

        search_users = rail.RepliconServicePageOperator(
            task_id="search_users",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('get_user_loginname')
                        }
                    }
                }
            },
            page_handler=python_callable_method.page_handler,
            all_result_data_handler=lambda result: python_callable_method.all_result_data_handler(
                result, rail.result('get_user_loginname'))
        )

        is_any_users_present = rail.IfOperator(
            task_id='is_any_users_present',
            test=lambda: len(rail.result('search_users')) > 0,
            yes_task="reenable_userprofile",
            no_task="execute_profile_creates"
        )

        reenable_userprofile = rail.RepliconServiceOperator(
            task_id='reenable_userprofile',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('search_users').0.useruri }}"
            }
        )

        remove_enddate = rail.RepliconServiceOperator(
            task_id='remove_enddate',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('search_users').0.useruri }}",
                },
                "modifications": {
                    "userDetailsToApply": {
                        "employmentEndDate": {
                            "date": None
                        }
                    }
                }
            }
        )

        update_user_sso_authentication = rail.RepliconServiceOperator(
            task_id='update_user_sso_authentication',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('search_users').0.useruri }}",
                "loginName": "{{ result('get_user_loginname') }}"
            }
        )

        execute_rehire_profile_updates = rail.TriggerDagRunForEachItemOperator(
            task_id='execute_rehire_profile_updates',
            retries=0,
            items=[0],
            trigger_dag_id=config.nrdc_updaterehiredisableuserbasicprofile,
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: request_payload.create_rehire_profile_payload(
                dag_run,
                rail.result("for_each_create_entry")['profile_type'],
                rail.result("for_each_create_entry")['is_primary'],
                rail.result("for_each_create_entry")['config'],
                rail.result("for_each_create_entry")['existing_primary']
            )
        )

        execute_profile_creates = rail.TriggerDagRunForEachItemOperator(
            task_id='execute_profile_creates',
            retries=0,
            items=[0],
            trigger_dag_id=config.nrdc_basicaddupdate,
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: request_payload.create_profile_payload(
                dag_run,
                rail.result("for_each_create_entry")['profile_type'],
                rail.result("for_each_create_entry")['is_primary'],
                rail.result("for_each_create_entry")['config']
            )
        )

        for_each_create_entry_end = rail.EmptyOperator(
            task_id='for_each_create_entry_end',
        )

        wait_for_profile_creates = rail.WaitForDagRunsSensor(
            task_id='wait_for_profile_creates',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("execute_profile_creates") or result("execute_rehire_profile_updates")}}'
        )

        check_substitute_needed = rail.IfOperator(
            task_id='check_substitute_needed',
            test=lambda: len(rail.result('analyze_profile_requirements')['actions']['substitute_assignments']) > 0,
            yes_task="search_all_users",
            no_task="catch_and_log_errors"
        )

        search_all_users = rail.RepliconServicePageOperator(
            task_id="search_all_users",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['logonname'].split('@')[0]
                        }
                    }
                }
            },
            page_handler=python_callable_method.page_handler,
            all_result_data_handler=python_callable_method.get_all_result_data_handler
        )

        for_each_entry_for_substitute = rail.ForEachOperator(
            task_id='for_each_entry_for_substitute',
            items=lambda: rail.result('analyze_profile_requirements')['actions']['substitute_assignments'],
            start_task="get_required_uris",
            end_task="for_each_entry_for_substitute_end"
        )

        def resolve_user_uri(dag_run,preferred_uri, fallback_type, fail_if_missing=True):
            if preferred_uri:
                return preferred_uri

            profile_suffix_map = {
                'C3': 'C3',
                'C4': 'Action Fund',
                'Delegate': 'Delegate'
            }

            suffix = ['lt', 'af', '']
            existing_login_names = [dag_run.conf['logonname'].split('@')[0] + name for name in suffix]

            search_result = rail.result('search_all_users')
            if search_result and isinstance(search_result, list):
                uri = [item['useruri'] for item in search_result if profile_suffix_map[fallback_type] in item['username'] and item['loginname'] in existing_login_names]
                return uri[0]

            if fail_if_missing:
                raise ValueError("Primary User is not available to add substitute user")
            return None


        get_required_uris = rail.PythonOperator(
            task_id='get_required_uris',
            python_callable=lambda dag_run: {
                "sub_uri": resolve_user_uri(
                    dag_run,
                    rail.result('for_each_entry_for_substitute').get('primary_uri'),
                    rail.result('for_each_entry_for_substitute').get('primary_profile')
                ),
                "actual_uri": resolve_user_uri(
                    dag_run,
                    rail.result('for_each_entry_for_substitute').get('target_uri'),
                    rail.result('for_each_entry_for_substitute').get('target_profile')
                )
            }
        )

        get_all_substitute_user_assignments = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('get_required_uris').actual_uri }}"
            },
            data_handler= lambda response: [item['user']['uri'] for item in response] if response else []
        )

        check_if_substitute_user_not_exists = rail.IfOperator(
            task_id='check_if_substitute_user_not_exists',
            test=lambda: rail.result('get_required_uris')['sub_uri'] not in rail.result('get_all_substitute_user_assignments'),
            yes_task="execute_substitute_assignments",
            no_task="for_each_entry_for_substitute_end"
        )

        execute_substitute_assignments = rail.TriggerDagRunForEachItemOperator(
            task_id='execute_substitute_assignments',
            retries=0,
            items=[0],
            trigger_dag_id=config.nrdc_assignsubstituteusersv2,
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('get_required_uris')['sub_uri'],
                "actualuri": rail.result('get_required_uris')['actual_uri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        for_each_entry_for_substitute_end = rail.EmptyOperator(
            task_id='for_each_entry_for_substitute_end',
        )

        if_any_child_dag_runs = rail.IfOperator(
            task_id='if_any_child_dag_runs',
            test=lambda: bool(rail.result('execute_substitute_assignments')),
            yes_task='wait_for_substitute_assignments',
            no_task='catch_and_log_errors'
        )

        wait_for_substitute_assignments = rail.WaitForDagRunsSensor(
            task_id='wait_for_substitute_assignments',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("execute_substitute_assignments") }}'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "user": "{{ dag_run.conf.firstname }}|{{ dag_run.conf.lastname }}|{{ dag_run.conf.emailaddress }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "action": "NA",
                "jobId": "{{ dag_run_ecid() }}"
            },
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> initialize_supervisors >> validate_input

        validate_input >> rail.Label('Yes') >> extract_current_profiles >> analyze_profile_requirements_task >> check_disable_needed
        validate_input >> rail.Label('No') >> log_validation_error >> catch_and_log_errors

        check_disable_needed >> rail.Label('Yes') >> execute_profile_disables >> wait_for_profile_disables >> check_update_needed
        check_disable_needed >> rail.Label('No') >> check_update_needed

        check_update_needed >> rail.Label('Yes') >> execute_profile_updates >> wait_for_profile_updates >> check_create_needed
        check_update_needed >> rail.Label('No') >> check_create_needed

        check_create_needed >> rail.Label('Yes') >> for_each_create_entry >> get_user_loginname >> search_users >> is_any_users_present
        check_create_needed >> rail.Label('No') >> check_substitute_needed

        is_any_users_present >> rail.Label('Yes') >> reenable_userprofile >> remove_enddate >> update_user_sso_authentication >> execute_rehire_profile_updates >> for_each_create_entry_end
        is_any_users_present >> rail.Label('No') >> execute_profile_creates >> for_each_create_entry_end >> wait_for_profile_creates >> check_substitute_needed
        for_each_create_entry >> for_each_create_entry_end

        check_substitute_needed >> rail.Label('Yes') >> search_all_users >> for_each_entry_for_substitute
        check_substitute_needed >> rail.Label('No') >> catch_and_log_errors

        for_each_entry_for_substitute >> get_required_uris >> get_all_substitute_user_assignments >> check_if_substitute_user_not_exists
        for_each_entry_for_substitute >> for_each_entry_for_substitute_end

        check_if_substitute_user_not_exists >> rail.Label('Yes') >> execute_substitute_assignments >> for_each_entry_for_substitute_end
        check_if_substitute_user_not_exists >> rail.Label('No') >> for_each_entry_for_substitute_end >> if_any_child_dag_runs

        if_any_child_dag_runs >> rail.Label('Yes') >> wait_for_substitute_assignments >> catch_and_log_errors
        if_any_child_dag_runs >> rail.Label('No') >> catch_and_log_errors

    return dag

rail.for_each_instance(create_dag)


from datetime import timedelta
import itertools
import pendulum
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'genoadesign_user_import_gdi_child_add_supervisor_v1_0_{config.instance}',
        description=f'Live|GDI_Child_Add Supervisor V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=config.schedule_interval,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_users_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_users_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def compose_user_details(response, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))
            return users_info[0] if users_info else None

        search_users_3 = rail.RepliconServicePageOperator(
            task_id='search_users_3',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                "sort": [],
                "filterExpression": {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['supervisorloginname'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['supervisorloginname'])
        )

        if_request_supervisornot_equals_to_dataloggerlog_getsupervisor_login_name_6 = rail.IfOperator(
            task_id='if_request_supervisornot_equals_to_dataloggerlog_getsupervisor_login_name_6',
            test='''{{ dag_run.conf.supervisorloginname != result('search_users_3').loginname }}''',
            yes_task="if_log_getsupervisor_uri_4_present_7",
            no_task="genoadi_user_import_logs_update_entry_35",
        )

        if_log_getsupervisor_uri_4_present_7 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_4_present_7',
            test='''{{ result('search_users_3').useruri | is_truthy }}''',
            yes_task="if_log_getsupervisor_status_8_equals_to_true_9",
            no_task="if_log_getsupervisor_uri_4_blank_29",
        )

        if_log_getsupervisor_status_8_equals_to_true_9 = rail.IfOperator(
            task_id='if_log_getsupervisor_status_8_equals_to_true_9',
            test='''{{ result('search_users_3').status | lower == 'true' }}''',
            yes_task="_adhoc_http_action_10",
            no_task="log_errorwhensupervisorisdisabled_28",
        )

        _adhoc_http_action_10 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_10',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_3').useruri }}"
            }
        )

        def get_supervision_permission(permission_task):
            permissionset = rail.find_first_by_attr_and_get_attr(rail.result(
                permission_task), 'policyUri', "urn:replicon:policy:supervision", 'permissionSet')
            return permissionset['uri'] if permissionset else None

        log_checkifthe_supervisor_permissionisassigned_11 = rail.PythonOperator(
            task_id='log_checkifthe_supervisor_permissionisassigned_11',
            python_callable=lambda: get_supervision_permission(
                '_adhoc_http_action_10')
        )

        if_log_checkifthe_supervisor_permissionisassigned_11_blank_12 = rail.IfOperator(
            task_id='if_log_checkifthe_supervisor_permissionisassigned_11_blank_12',
            test='''{{ result('log_checkifthe_supervisor_permissionisassigned_11') | is_falsy }}''',
            yes_task="_adhoc_http_action_13",
            no_task="if_request_action_equals_to_add_18",
        )

        _adhoc_http_action_13 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_13',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        log_requiredpermissiontoassign_14 = rail.PythonOperator(
            task_id='log_requiredpermissiontoassign_14',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_13'), 'displayText', "Supervisor", 'uri')
        )

        if_log_requiredpermissiontoassign_14_present_15 = rail.IfOperator(
            task_id='if_log_requiredpermissiontoassign_14_present_15',
            test='''{{ result('log_requiredpermissiontoassign_14') | is_truthy }}''',
            yes_task="assign_supervsior_permission_set_to_user_16",
            no_task="if_request_action_equals_to_add_18",
        )

        assign_supervsior_permission_set_to_user_16 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_16',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_3').useruri }}",
                "permissionSetUri": "{{ result('log_requiredpermissiontoassign_14') }}"
            }
        )

        if_request_action_equals_to_add_18 = rail.IfOperator(
            task_id='if_request_action_equals_to_add_18',
            test='''{{ dag_run.conf.action == 'Add' }}''',
            yes_task="update_initial_supervisor_19",
            no_task="if_request_action_equals_to_update_20",
        )

        update_initial_supervisor_19 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_19',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('search_users_3').useruri }}",
                "scheduleEntries": []
            }
        )

        if_request_action_equals_to_update_20 = rail.IfOperator(
            task_id='if_request_action_equals_to_update_20',
            test='''{{ dag_run.conf.action == 'Update' }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_24",
            no_task="if_log_getsupervisor_uri_4_blank_29",
        )

        def get_supervisor_effective_date(dag_run):
            effective_date = dag_run.conf['supervisoreffectivedate']
            if effective_date:
                effective_date = pendulum.now(config.pacific_timezone)
            return {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            }

        update_supervisor_assignment_schedule_over_date_range_24 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_24',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_users_3')['useruri'],
                "dateRange": {
                    "startDate": get_supervisor_effective_date(dag_run),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_errorwhensupervisorisdisabled_28 = rail.PythonOperator(
            task_id='log_errorwhensupervisorisdisabled_28',
            python_callable=lambda dag_run:  "Supervsior assignment/update is not done for user " +
            dag_run.conf['loginname'] + " as supervsior with login name " +
            dag_run.conf['supervisorloginname'] + " is disabled in Replicon."
        )

        if_log_getsupervisor_uri_4_blank_29 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_4_blank_29',
            test='''{{ result('search_users_3').useruri | is_falsy }}''',
            yes_task="log_erroras_supervisorisnotavailable_30",
            no_task="log_errorfor_supervisorand_userslogin_nameissame_32",
        )

        log_erroras_supervisorisnotavailable_30 = rail.PythonOperator(
            task_id='log_erroras_supervisorisnotavailable_30',
            python_callable=lambda dag_run:  "Supervisor is not updated as the supervisor  with login name " +
            dag_run.conf['supervisorloginname']+" is not available"
        )

        log_errorfor_supervisorand_userslogin_nameissame_32 = rail.PythonOperator(
            task_id='log_errorfor_supervisorand_userslogin_nameissame_32',
            python_callable=lambda dag_run:  "Supervisor is not updated as the " +
            dag_run.conf['loginname'] +
            " for user and supervisor is same on the input file"
        )

        genoadi_user_import_logs_update_entry_35 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_update_entry_35',
            message="na",
            severity='\
                    {%- if result("log_errorfor_supervisorand_userslogin_nameissame_32") | is_truthy or result("log_erroras_supervisorisnotavailable_30") | is_truthy or result("log_errorwhensupervisorisdisabled_28") | is_truthy -%} \
                        Failed\
                    {%- else -%} \
                        Success\
                    {%- endif -%}',
            properties={
                "username|loginname": "{{ dag_run.conf.username }} |{{ dag_run.conf.loginname }}",
                "status": '\
                    {%- if result("log_errorfor_supervisorand_userslogin_nameissame_32") | is_truthy or result("log_erroras_supervisorisnotavailable_30") | is_truthy or result("log_errorwhensupervisorisdisabled_28") | is_truthy -%} \
                        Failed\
                    {%- else -%} \
                        Success\
                    {%- endif -%}',
                "details": '''{{ [result("log_errorfor_supervisorand_userslogin_nameissame_32"), result("log_erroras_supervisorisnotavailable_30"), result("log_errorwhensupervisorisdisabled_28")] | smartjoin_by_delim(';')}}''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        genoadi_user_import_logs_update_entry_39 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_update_entry_39',
            message="{{ get_error_message() }}",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "username|loginname": "{{ dag_run.conf.username }}|{{ dag_run.conf.loginname }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> search_users_3
        search_users_3 >> if_request_supervisornot_equals_to_dataloggerlog_getsupervisor_login_name_6
        if_request_supervisornot_equals_to_dataloggerlog_getsupervisor_login_name_6 >> rail.Label('No') >> \
            genoadi_user_import_logs_update_entry_35
        if_request_supervisornot_equals_to_dataloggerlog_getsupervisor_login_name_6 >> rail.Label(
            'Yes') >> if_log_getsupervisor_uri_4_present_7
        if_log_getsupervisor_uri_4_present_7 >> rail.Label(
            'Yes') >> if_log_getsupervisor_status_8_equals_to_true_9
        if_log_getsupervisor_status_8_equals_to_true_9 >> rail.Label('No') >> log_errorwhensupervisorisdisabled_28 >> \
            if_log_getsupervisor_uri_4_blank_29
        if_log_getsupervisor_status_8_equals_to_true_9 >> rail.Label(
            'Yes') >> _adhoc_http_action_10 >> log_checkifthe_supervisor_permissionisassigned_11 >> \
            if_log_checkifthe_supervisor_permissionisassigned_11_blank_12
        if_log_checkifthe_supervisor_permissionisassigned_11_blank_12 >> rail.Label(
            'Yes') >> _adhoc_http_action_13 >> log_requiredpermissiontoassign_14 >> \
            if_log_requiredpermissiontoassign_14_present_15
        if_log_requiredpermissiontoassign_14_present_15 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_16 >> if_request_action_equals_to_add_18
        if_log_requiredpermissiontoassign_14_present_15 >> rail.Label(
            'No') >> if_request_action_equals_to_add_18
        if_log_checkifthe_supervisor_permissionisassigned_11_blank_12 >> rail.Label(
            'No') >> if_request_action_equals_to_add_18
        if_request_action_equals_to_add_18 >> rail.Label(
            'Yes') >> update_initial_supervisor_19 >> if_request_action_equals_to_update_20
        if_request_action_equals_to_add_18 >> rail.Label(
            'No') >> if_request_action_equals_to_update_20
        if_request_action_equals_to_update_20 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_24 >> if_log_getsupervisor_uri_4_blank_29
        if_request_action_equals_to_update_20 >> rail.Label(
            'No') >> if_log_getsupervisor_uri_4_blank_29
        if_log_getsupervisor_uri_4_present_7 >> rail.Label(
            'No') >> if_log_getsupervisor_uri_4_blank_29
        if_log_getsupervisor_uri_4_blank_29 >> rail.Label(
            'Yes') >> log_erroras_supervisorisnotavailable_30 >> genoadi_user_import_logs_update_entry_35
        if_log_getsupervisor_uri_4_blank_29 >> rail.Label(
            'No') >> log_errorfor_supervisorand_userslogin_nameissame_32 >> \
            genoadi_user_import_logs_update_entry_35 >> genoadi_user_import_logs_update_entry_39 >> \
            log_to_sumo

    return dag


rail.for_each_instance(create_dag)

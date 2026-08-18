
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.supervisor_assignment_child_dag_id,
        description=f'VelawG3_Child_Supervisor Assignment_V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='velaw_supervisor_assignment_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='velaw_supervisor_assignment_logs',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        velaw_supervisor_assignment_logs = rail.CreateLogOperator(
            task_id='velaw_supervisor_assignment_logs',
        )

        def map_supervisor_listdata(response, dag_run):
            def get_today_date():
                today = datetime.now()
                if today.weekday() == 0:
                    datestr = (today - timedelta(days=1)).strftime("%d/%m/%Y")
                elif today.weekday() == 1:
                    datestr = (today - timedelta(days=2)).strftime("%d/%m/%Y")
                elif today.weekday() == 2:
                    datestr = (today - timedelta(days=3)).strftime("%d/%m/%Y")
                elif today.weekday() == 3:
                    datestr = (today - timedelta(days=4)).strftime("%d/%m/%Y")
                elif today.weekday() == 4:
                    datestr = (today - timedelta(days=5)).strftime("%d/%m/%Y")
                elif today.weekday() == 5:
                    datestr = (today - timedelta(days=6)).strftime("%d/%m/%Y")
                else:
                    datestr = today.strftime("%d/%m/%Y")
                return datestr

            supervisor_list = list(map(lambda item: {
                'name': item['cells'][0]['textValue'],
                'loginname': item['cells'][1]['textValue'],
                'uri': item['cells'][0]['uri'],
                'status': item['cells'][3]['textValue']
            }, response['rows'])) if response['rows'] else []

            supervisor = list(filter(
                lambda x: x['employeeid'] == dag_run.conf['supervisorloginname'], supervisor_list)) if supervisor_list else []
            return {
                'name': supervisor[0]['name'] if supervisor else '',
                'uri': supervisor[0]['uri'] if supervisor else '',
                'status': supervisor[0]['status'].lower() if supervisor else '',
                "today": get_today_date
            } if supervisor else []

        search_users_3 = rail.RepliconServiceOperator(
            task_id='search_users_3',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                'page': '1',
                'pagesize': '100',
                'columnUris': [
                    'urn:replicon:user-list-column:login-name'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['supervisorloginname']
                        }
                    }
                }
            },
            data_handler=map_supervisor_listdata
        )

        if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_5 = rail.IfOperator(
            task_id='if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_5',
            test='''{{ dag_run.conf.supervisorloginname != dag_run.conf.loginname }}''',
            yes_task="if_output_urioutput_present_6",
            no_task="log_errorfor_supervisorand_userslogin_nameissame_27",
        )

        if_output_urioutput_present_6 = rail.IfOperator(
            task_id='if_output_urioutput_present_6',
            test=lambda: rail.result('search_users_3') and rail.result(
                'search_users_3')['uri'],
            yes_task="if_output_statusoutput_equals_to_true_7",
            no_task="if_output_urioutput_blank_24"
        )

        if_output_statusoutput_equals_to_true_7 = rail.IfOperator(
            task_id='if_output_statusoutput_equals_to_true_7',
            test=lambda: rail.result('search_users_3') and rail.result(
                'search_users_3')['status'] == 'True',
            yes_task="_adhoc_http_action_9",
            no_task="log_errorwhensupervisorisdisabled_23",
        )

        _adhoc_http_action_9 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_9',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_3').uri }}"
            }
        )

        log_checkifthe_supervisor_permissionisassigned_10 = rail.PythonOperator(
            task_id='log_checkifthe_supervisor_permissionisassigned_10',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('adhoc_http_action_9')[
                                                                         'permissionSet'], 'name', "*Gen3 - Supervisor", 'uri') if rail.result('adhoc_http_action_9')[0]['policyUri'] else null
        )

        if_log_checkifthe_supervisor_permissionisassigned_10_blank_11 = rail.IfOperator(
            task_id='if_log_checkifthe_supervisor_permissionisassigned_10_blank_11',
            test=lambda: not rail.result(
                'log_checkifthe_supervisor_permissionisassigned_10'),
            yes_task="assign_supervsior_permission_set_to_user_gen3_supervisor_12",
            no_task="log_checkiftheenduser_permissionforsupervisorisassigned_13",
        )

        assign_supervsior_permission_set_to_user_gen3_supervisor_12 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_gen3_supervisor_12',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_3').uri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        log_checkiftheenduser_permissionforsupervisorisassigned_13 = rail.PythonOperator(
            task_id='log_checkiftheenduser_permissionforsupervisorisassigned_13',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('adhoc_http_action_9')[
                                                                         'permissionSet'], 'name', "*Gen3 - Project Resource with reports & Substitute User", 'uri') if rail.result('adhoc_http_action_9')[0]['policyUri'] else null
        )

        if_log_checkiftheenduser_permissionforsupervisorisassigned_13_blank_14 = rail.IfOperator(
            task_id='if_log_checkiftheenduser_permissionforsupervisorisassigned_13_blank_14',
            test=lambda: not rail.result(
                'log_checkiftheenduser_permissionforsupervisorisassigned_13'),
            yes_task="assign_supervsior_permission_set_to_user_gen3_project_resourcewithreports_substitute_user_15",
            no_task="if_request_action_equals_to_add_16",
        )

        assign_supervsior_permission_set_to_user_gen3_project_resourcewithreports_substitute_user_15 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_gen3_project_resourcewithreports_substitute_user_15',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_3').uri }}",
                "permissionSetUri": "{{ dag_run.conf.enduserpermissionformanager }}"
            }
        )

        if_request_action_equals_to_add_16 = rail.IfOperator(
            task_id='if_request_action_equals_to_add_16',
            test='''{{ dag_run.conf.action == 'Add' }}''',
            yes_task="update_initial_supervisor_17",
            no_task="if_request_action_equals_to_update_18",
        )

        update_initial_supervisor_17 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_17',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('search_users_3').uri }}",
                "scheduleEntries": []
            }
        )

        if_request_action_equals_to_update_18 = rail.IfOperator(
            task_id='if_request_action_equals_to_update_18',
            test='''{{ dag_run.conf.action == 'Update' }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_19",
            no_task="if_output_urioutput_blank_24",
        )

        update_supervisor_assignment_schedule_over_date_range_19 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_19',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_users_3')['uri'],
                "dateRange": {
                    "startDate": {
                        "year": int(rail.result('search_users_3')['today'].split('/')[2]),
                        "month": int(rail.result('search_users_3')['today'].split('/')[1]),
                        "day": int(rail.result('search_users_3')['today'].split('/')[0])
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_errorwhensupervisorisdisabled_23 = rail.PythonOperator(
            task_id='log_errorwhensupervisorisdisabled_23',
            python_callable=lambda: "Supervsior assignment/update is not done for user {{dag_run.conf.loginname}} as supervsior with loginname {{dag_run.conf.supervisorloginname}} is disabled in Replicon."
        )

        if_output_urioutput_blank_24 = rail.IfOperator(
            task_id='if_output_urioutput_blank_24',
            test=lambda: not rail.result('search_users_3'),
            yes_task="log_erroras_supervisorisnotavailable_25",
            no_task="velaw_user_import_logs_update_entry_30",
        )

        log_erroras_supervisorisnotavailable_25 = rail.PythonOperator(
            task_id='log_erroras_supervisorisnotavailable_25',
            python_callable=lambda: "Supervisor is not updated as the supervisor with login name {{dag_run.conf.supervisorloginname}} is not available"
        )

        log_errorfor_supervisorand_userslogin_nameissame_27 = rail.PythonOperator(
            task_id='log_errorfor_supervisorand_userslogin_nameissame_27',
            python_callable=lambda: "Supervisor is not updated as the \"Login name\" for user and supervisor is same on the input file"
        )

        def get_details():
            message_list = []
            if rail.result('log_errorfor_supervisorand_userslogin_nameissame_27'):
                message_list.append(rail.result(
                    'log_errorfor_supervisorand_userslogin_nameissame_27'))
            if rail.result('log_errorwhensupervisorisdisabled_23'):
                message_list.append(rail.result(
                    'log_errorwhensupervisorisdisabled_23'))
            if rail.result('log_erroras_supervisorisnotavailable_25'):
                message_list.append(rail.result(
                    'log_erroras_supervisorisnotavailable_25'))

            return ','.join(message_list) if message_list else ''

        velaw_user_import_logs_update_entry_30 = rail.WriteLogOperator(
            task_id='velaw_user_import_logs_update_entry_30',
            log="{{ result('velaw_supervisor_assignment_logs') }}",
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "username": dag_run.conf['username'],
                "loginname": dag_run.conf['loginname'],
                "employeeid": dag_run.conf['employeeid'],
                "importaction": dag_run.conf['action'],
                "status": "Error" if dag_run.conf['status'] == "Error" else "Exception" if rail.result('log_errorfor_supervisorand_userslogin_nameissame_27') or rail.result('log_errorwhensupervisorisdisabled_23') or rail.result('log_erroras_supervisorisnotavailable_25') else dag_run.conf['status'],
                "details": get_details()
            }
        )

        velaw_supervisor_logs_exception_37 = rail.WriteLogOperator(
            task_id='velaw_supervisor_logs_exception_37',
            log="{{ result('velaw_supervisor_assignment_logs') }}",
            message="na",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "username": "{{ dag_run.conf.username }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "importaction": "{{ dag_run.conf.action }}",
                "status": "Error",
                "details": "{{ dag_run.conf.employeeid}} ', ' {{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> velaw_supervisor_assignment_logs \
            >> search_users_3 >> if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_5
        if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_5 >> rail.Label(
            'Yes') >> if_output_urioutput_present_6
        if_request_supervisorloginname_not_equals_to_dataworkato_service3cd9c331requestloginname_5 >> rail.Label(
            'No') >> log_errorfor_supervisorand_userslogin_nameissame_27 >> velaw_user_import_logs_update_entry_30
        if_output_urioutput_present_6 >> rail.Label(
            'Yes') >> if_output_statusoutput_equals_to_true_7
        if_output_statusoutput_equals_to_true_7 >> rail.Label(
            'Yes') >> _adhoc_http_action_9 >> log_checkifthe_supervisor_permissionisassigned_10 >> if_log_checkifthe_supervisor_permissionisassigned_10_blank_11
        if_output_statusoutput_equals_to_true_7 >> rail.Label(
            'No') >> log_errorwhensupervisorisdisabled_23 >> if_output_urioutput_blank_24
        if_log_checkifthe_supervisor_permissionisassigned_10_blank_11 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_gen3_supervisor_12 >> log_checkiftheenduser_permissionforsupervisorisassigned_13
        if_log_checkifthe_supervisor_permissionisassigned_10_blank_11 >> rail.Label(
            'No') >> log_checkiftheenduser_permissionforsupervisorisassigned_13 >> if_log_checkiftheenduser_permissionforsupervisorisassigned_13_blank_14
        if_log_checkiftheenduser_permissionforsupervisorisassigned_13_blank_14 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_gen3_project_resourcewithreports_substitute_user_15 >> if_request_action_equals_to_add_16
        if_log_checkiftheenduser_permissionforsupervisorisassigned_13_blank_14 >> rail.Label(
            'No') >> if_request_action_equals_to_add_16
        if_request_action_equals_to_add_16 >> rail.Label(
            'Yes') >> update_initial_supervisor_17 >> if_request_action_equals_to_update_18
        if_request_action_equals_to_add_16 >> rail.Label(
            'No') >> if_request_action_equals_to_update_18
        if_request_action_equals_to_update_18 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_19 >> if_output_urioutput_blank_24
        if_request_action_equals_to_update_18 >> rail.Label(
            'No') >> if_output_urioutput_blank_24
        if_output_urioutput_present_6 >> rail.Label(
            'No') >> if_output_urioutput_blank_24
        if_output_urioutput_blank_24 >> rail.Label(
            'Yes') >> log_erroras_supervisorisnotavailable_25 >> velaw_user_import_logs_update_entry_30
        if_output_urioutput_blank_24 >> rail.Label(
            'No') >> velaw_user_import_logs_update_entry_30 >> velaw_supervisor_logs_exception_37 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)

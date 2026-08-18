
from datetime import datetime, timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'horizonmedia_user_import_supervisor_assignment_child_{config.instance}',
        description=f'Horizonmedia - Child_Supervisor Assignment V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_users_3 = rail.RepliconServiceOperator(
            task_id='search_users_3',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": "{{ dag_run.conf.supervisorempid }}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        invoke_custom_ruby_code_4 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_4',
            python_callable=lambda: next(filter(lambda x: x['employeeid'] == rail.get_dag_run_conf()['supervisorempid'], map(lambda x: {
                "name": x['cells'][0]['textValue'],
                "loginname": x['cells'][1]['textValue'],
                "uri": x['cells'][0]['uri'],
                "status": x['cells'][3]['textValue'],
                "employeeid": x['cells'][2]['textValue']
            }, rail.result('search_users_3')['rows'])), null)
        )

        date_split_5 = rail.PythonOperator(
            task_id='date_split_5',
            python_callable=lambda: rail.get_dag_run_conf()['supeffectivedate']
        )

        if_request_supervisorempid_not_equals_to_dataworkato_service3cd9c331requestuserid_6 = rail.IfOperator(
            task_id='if_request_supervisorempid_not_equals_to_dataworkato_service3cd9c331requestuserid_6',
            test='''{{ dag_run.conf.supervisorempid != dag_run.conf.userid }}''',
            yes_task="if_output_urioutput_present_7",
            no_task="log_errorfor_supervisorand_userslogin_nameissame_29",
        )

        if_output_urioutput_present_7 = rail.IfOperator(
            task_id='if_output_urioutput_present_7',
            test='''{{ result('invoke_custom_ruby_code_4') | is_truthy }}''',
            yes_task="get_permission_for_supervisor",
            no_task="if_output_urioutput_blank_26",
        )

        get_permission_for_supervisor = rail.RepliconServiceOperator(
            task_id='get_permission_for_supervisor',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('invoke_custom_ruby_code_4').uri }}"
            }
        )

        log_checkifthe_supervisor_permissionisassigned_10 = rail.PythonOperator(
            task_id='log_checkifthe_supervisor_permissionisassigned_10',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_permission_for_supervisor'), 'permissionSet.displayText', "SUPERVISOR", 'uri')
        )

        get_userdataforsupervisor_11 = rail.RepliconServiceOperator(
            task_id='get_userdataforsupervisor_11',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('invoke_custom_ruby_code_4').uri }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_log_checkifthe_supervisor_permissionisassigned_10_blank_12 = rail.IfOperator(
            task_id='if_log_checkifthe_supervisor_permissionisassigned_10_blank_12',
            test='''{{ result('log_checkifthe_supervisor_permissionisassigned_10') | is_falsy and result('get_userdataforsupervisor_11')[0].userDetails.customFieldValues | find_first_by_attr_and_get_attr("customField.displayText",'Manager',"text") | matches('Yes')  }}''',
            yes_task="assign_supervsior_permission_set_to_user_supervisor_13",
            no_task="if_request_action_equals_to_add_18",
        )

        assign_supervsior_permission_set_to_user_supervisor_13 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_supervisor_13',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('invoke_custom_ruby_code_4').uri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        assign_supervsior_permission_set_to_user_teammanager_14 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_teammanager_14',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('invoke_custom_ruby_code_4').uri }}",
                "permissionSetUri": "{{ dag_run.conf.teammanagerpermission }}"
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
                "initialSupervisorUri": "{{ result('invoke_custom_ruby_code_4').uri }}",
                "scheduleEntries": []
            }
        )

        if_request_action_equals_to_update_20 = rail.IfOperator(
            task_id='if_request_action_equals_to_update_20',
            test='''{{ dag_run.conf.action == 'Update' }}''',
            yes_task="get_timesheet_periods_for_user_21",
            no_task="if_output_urioutput_blank_26",
        )

        get_timesheet_periods_for_user_21 = rail.RepliconServiceOperator(
            task_id='get_timesheet_periods_for_user_21',
            endpoint="/services/TimesheetPeriodService1.svc/GetTimesheetPeriodsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('date_split_5').year }}",
                        "month": "{{ result('date_split_5').month }}",
                        "day": "{{ result('date_split_5').day }}",
                    },
                    "endDate":  {
                        "year": "{{ result('date_split_5').year }}",
                        "month": "{{ result('date_split_5').month }}",
                        "day": "{{ result('date_split_5').day }}",
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        date_split_supervisoreffectivedate_22 = rail.PythonOperator(
            task_id='date_split_supervisoreffectivedate_22',
            python_callable=lambda: rail.result('get_timesheet_periods_for_user_21')[0]['dateRange']['startDate'] if rail.result('get_timesheet_periods_for_user_21')
            and rail.result('get_timesheet_periods_for_user_21')[0]['dateRange']['startDate'] else {
                "day": datetime.utcnow().day, "month": datetime.utcnow().month, "year": datetime.utcnow().year}
        )

        update_supervisor_assignment_schedule_over_date_range_23 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_23',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('invoke_custom_ruby_code_4').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('date_split_supervisoreffectivedate_22').year }}",
                        "month": "{{ result('date_split_supervisoreffectivedate_22').month }}",
                        "day": "{{ result('date_split_supervisoreffectivedate_22').day }}",
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_output_urioutput_blank_26 = rail.IfOperator(
            task_id='if_output_urioutput_blank_26',
            test='''{{ result('invoke_custom_ruby_code_4') | is_falsy or result('invoke_custom_ruby_code_4').uri | is_falsy }}''',
            yes_task="log_erroras_supervisorisnotavailable_27",
            no_task="get_exception_log",
        )

        log_erroras_supervisorisnotavailable_27 = rail.PythonOperator(
            task_id='log_erroras_supervisorisnotavailable_27',
            python_callable=lambda:  f'''Supervisor is not updated as the supervisor with employee id "{rail.get_dag_run_conf()['supervisorempid'] }" is not available'''
        )

        log_errorfor_supervisorand_userslogin_nameissame_29 = rail.PythonOperator(
            task_id='log_errorfor_supervisorand_userslogin_nameissame_29',
            python_callable=lambda:  '''"Supervisor is not updated as the Employee ID for user and supervisor is same on the input file"'''
        )

        get_exception_log = rail.PythonOperator(
            task_id='get_exception_log',
            python_callable=lambda: rail.result('log_erroras_supervisorisnotavailable_27') or rail.result(
                'log_errorfor_supervisorand_userslogin_nameissame_29') or ''
        )

        add_log = rail.WriteLogOperator(
            task_id='add_log',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity='''{{ "Exception" if result('get_exception_log') | is_truthy  else  "Success" }}''',
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "username": "{{ dag_run.conf.username }}",
                "action": "{{ dag_run.conf.action }}",
                "status": '''{{ "Exception" if result('get_exception_log') | is_truthy  else  "Success" }}''',
                "details": '''{{ "Supervisor assigment is failed - " + result('get_exception_log') if result('get_exception_log') | is_truthy else "Supervisor assigment is successful"}}''',
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "username": "{{ dag_run.conf.username }}",
                "action": "{{ dag_run.conf.action }}",
                "status": "Error",
                "details": '{{ get_error_message() }}',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> search_users_3
        search_users_3 >> invoke_custom_ruby_code_4 >> date_split_5 >> if_request_supervisorempid_not_equals_to_dataworkato_service3cd9c331requestuserid_6
        if_request_supervisorempid_not_equals_to_dataworkato_service3cd9c331requestuserid_6 >> rail.Label(
            'Yes') >> if_output_urioutput_present_7
        if_output_urioutput_present_7 >> rail.Label(
            'Yes') >> get_permission_for_supervisor >> log_checkifthe_supervisor_permissionisassigned_10 >> get_userdataforsupervisor_11 >> if_log_checkifthe_supervisor_permissionisassigned_10_blank_12
        if_log_checkifthe_supervisor_permissionisassigned_10_blank_12 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_supervisor_13 >> assign_supervsior_permission_set_to_user_teammanager_14 >> if_request_action_equals_to_add_18
        if_log_checkifthe_supervisor_permissionisassigned_10_blank_12 >> rail.Label(
            'No') >> if_request_action_equals_to_add_18
        if_request_action_equals_to_add_18 >> rail.Label(
            'Yes') >> update_initial_supervisor_19 >> if_request_action_equals_to_update_20
        if_request_action_equals_to_add_18 >> rail.Label(
            'No') >> if_request_action_equals_to_update_20
        if_request_action_equals_to_update_20 >> rail.Label(
            'Yes') >> get_timesheet_periods_for_user_21 >> date_split_supervisoreffectivedate_22 >> update_supervisor_assignment_schedule_over_date_range_23 >> if_output_urioutput_blank_26
        if_request_action_equals_to_update_20 >> rail.Label(
            'No') >> if_output_urioutput_blank_26
        if_output_urioutput_present_7 >> rail.Label(
            'No') >> if_output_urioutput_blank_26
        if_output_urioutput_blank_26 >> rail.Label(
            'Yes') >> log_erroras_supervisorisnotavailable_27 >> get_exception_log
        if_output_urioutput_blank_26 >> rail.Label(
            'No') >> get_exception_log
        if_request_supervisorempid_not_equals_to_dataworkato_service3cd9c331requestuserid_6 >> rail.Label(
            'No') >> log_errorfor_supervisorand_userslogin_nameissame_29 >> get_exception_log
        get_exception_log >> add_log >> finish >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)

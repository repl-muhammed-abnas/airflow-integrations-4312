from datetime import datetime as dt, timedelta
import rail
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'statestreet_user_sync_supervisorassignment_child_{config.instance}',
        description=f'Statestreet_user_sync_supervisorassignment_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_childs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_details_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_details_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        get_user_details_3 = rail.RepliconServiceOperator(
            task_id='get_user_details_3',
            endpoint="/services/userService1.svc/GetUserDetails",
            data={
                "userUri": "{{ dag_run.conf.useruri}}"
            }
        )

        if_request_managerid_present_7 = rail.IfOperator(
            task_id='if_request_managerid_present_7',
            test='''{{ dag_run.conf.manager_id | is_truthy }}''',
            yes_task="get_enabled_users_8",
            no_task="catch_35",
        )

        get_enabled_users_8 = rail.RepliconServiceOperator(
            task_id='get_enabled_users_8',
            endpoint="/services/userlistService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
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
                                "text": "{{ dag_run.conf.manager_id}}",
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "dateTimeUtc": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [],
                                "bool": "true",
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "dateTimeUtc": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        if_first_datatype_present_9 = rail.IfOperator(
            task_id='if_first_datatype_present_9',
            # pylint: disable=too-many-statements line-too-long
            test=lambda: (rail.result('get_enabled_users_8')['rows'][0]['cells'][0]['dataType']) if (rail.result('get_enabled_users_8')['rows']) and (rail.result('get_enabled_users_8')[
                'rows'][0]) and (rail.result('get_enabled_users_8')['rows'][0]['cells'][0]) and (rail.result('get_enabled_users_8')['rows'][0]['cells'][0]['dataType']) else None,
            yes_task="foreach_d_10",
            no_task="if_log_supervisor_uri_17_present_18",
        )

        foreach_d_10 = rail.ForEachOperator(
            task_id='foreach_d_10',
            items="{{ result('get_enabled_users_8').rows | to_json }}",
            start_task='accumulate_list_items_11',
            end_task='foreach_d_10_end'
        )

        accumulate_list_items_11 = rail.SetVariableOperator(
            task_id='accumulate_list_items_11',
            name='Supervisors',
            append=True,
            value=lambda: {
                "name": rail.find_first_by_attr_and_get_attr(rail.result('foreach_d_10')['cells'],
                                                             'objectType', 'urn:replicon:object-type:user', 'textValue', ''),
                "employeeid": rail.find_first_by_attr_and_get_attr(rail.result('foreach_d_10')['cells'],
                                                                   'dataType', 'urn:replicon:list-type:string', 'textValue', ''),
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('foreach_d_10')['cells'],
                                                            'objectType', 'urn:replicon:object-type:user', 'uri', '')
            }
        )

        foreach_d_10_end = rail.EmptyOperator(
            task_id='foreach_d_10_end',
        )

        def get_supervisorcount(dag_run):
            record_data = rail.result('accumulate_list_items_11')['value']
            list_count = record_data if record_data else None
            supervisor2 = ''
            count = 0
            for data in list_count:
                if data['employeeid'] == dag_run.conf['manager_id']:
                    supervisor2 = data['uri']
                    count += 1
            return {
                'count': count,
                'uri_count': supervisor2
            }

        log_supervisorcount_12 = rail.PythonOperator(
            task_id='log_supervisorcount_12',
            python_callable=get_supervisorcount
        )

        if_log_supervisorcount_12_greater_than_1_13 = rail.IfOperator(
            task_id='if_log_supervisorcount_12_greater_than_1_13',
            test="{{result('log_supervisorcount_12').count > 1}}",
            yes_task="statestreet_userimport_logs_add_entry_14",
            no_task="if_log_supervisorcount_12_equals_to_1_16",
        )

        statestreet_userimport_logs_add_entry_14 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_14',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                " Multiple Users found with Manager ID" +
                    dag_run.conf['manager_id'],
                "field_name": dag_run.conf['login_id'] + "|" + dag_run.conf['emp_id'] + "|" + rail.result('get_user_details_3')['displayText'],
                "status": "Failed",
            }
        )

        if_log_supervisorcount_12_equals_to_1_16 = rail.IfOperator(
            task_id='if_log_supervisorcount_12_equals_to_1_16',
            test="{{result('log_supervisorcount_12').count == 1}}",
            yes_task="log_supervisor_uri_17",
            no_task="if_log_supervisor_uri_17_present_18",
        )

        log_supervisor_uri_17 = rail.PythonOperator(
            task_id='log_supervisor_uri_17',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'accumulate_list_items_11')['value'], 'employeeid', dag_run.conf['manager_id'], 'uri', '')
        )

        if_log_supervisor_uri_17_present_18 = rail.IfOperator(
            task_id='if_log_supervisor_uri_17_present_18',
            test='''{{ result('log_supervisor_uri_17') | is_truthy }}''',
            yes_task="if_log_supervisor_uri_17_not_equals_to_datarestget_user_details_3responsedsupervisoruri_19",
            no_task="statestreet_userimport_logs_add_entry_33",
        )

        if_log_supervisor_uri_17_not_equals_to_datarestget_user_details_3responsedsupervisoruri_19 = rail.IfOperator(
            task_id='if_log_supervisor_uri_17_not_equals_to_datarestget_user_details_3responsedsupervisoruri_19',
            test=lambda: rail.result('log_supervisor_uri_17') != (rail.result('get_user_details_3')['supervisor']['uri'] if rail.result(
                'get_user_details_3')['supervisor'] and rail.result('get_user_details_3')['supervisor']['uri'] else null),
            yes_task="get_assigned_permission_sets_for_user2_20",
            no_task="catch_35"
        )

        get_assigned_permission_sets_for_user2_20 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_20',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('log_supervisor_uri_17') }}"
            }
        )

        log_check_supervisor_permission_21 = rail.PythonOperator(
            # pylint: disable=too-many-statements line-too-long
            task_id='log_check_supervisor_permission_21',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_permission_sets_for_user2_20'), 'policyUri',
                'urn:replicon:policy:supervision', 'user.displayText', '') if rail.result('get_assigned_permission_sets_for_user2_20') and rail.result('get_assigned_permission_sets_for_user2_20')[0]['policyUri'] else None
        )

        def get_todays_date():
            date_now = dt.utcnow()
            return {
                "year": date_now.year,
                "month": date_now.month,
                "day": date_now.day
            }

        log_todays_date = rail.PythonOperator(
            task_id='log_todays_date',
            python_callable=get_todays_date
        )

        if_log_check_supervisor_permission_21_present_22 = rail.IfOperator(
            task_id='if_log_check_supervisor_permission_21_present_22',
            test='''{{ result('log_check_supervisor_permission_21') | is_truthy }}''',
            yes_task="apply_user_modifications_23",
            no_task="statestreet_userimport_logs_add_entry_31",
        )

        apply_user_modifications_23 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications_23',
            endpoint="/services/importService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": {
                        "scheduleEntriesToAdd": [
                            {
                                "supervisor": {
                                    "uri": "{{ result('log_supervisor_uri_17') }}",
                                    "loginName": null,
                                    "parameterCorrelationId": null
                                },
                                "effectiveDate": {
                                    "year": "{{result('log_todays_date').year}}",
                                    "month": "{{result('log_todays_date').month}}",
                                    "day": "{{result('log_todays_date').day}}",
                                }
                            }
                        ],
                        "scheduleEntriesToPut": []
                    },
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        if_d_errors_present_24 = rail.IfOperator(
            task_id='if_d_errors_present_24',
            test='''{{ result('apply_user_modifications_23').errors | is_truthy }}''',
            yes_task="log_errormessage_25",
            no_task="statestreet_userimport_logs_add_entry_28",
        )

        log_errormessage_25 = rail.PythonOperator(
            task_id='log_errormessage_25',
            python_callable=lambda: rail.result('apply_user_modifications_23')[
                'user']['displayText'] if rail.result('apply_user_modifications_23')['errors'] else None
        )

        statestreet_userimport_logs_add_entry_26 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_26',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User Supervisor -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Supervisor not updated" +
                    rail.result('log_errormessage_25') +
                dag_run.conf['manager_id'],
                "field_name": dag_run.conf['login_id'] + "|" + dag_run.conf['emp_id'] + "|" +
                rail.result('get_user_details_3')['displayText'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_28 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_28',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User Supervisor -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Supervisor Updated to" + dag_run.conf['manager_id'],
                "field_name": dag_run.conf['login_id'] + "|" + dag_run.conf['emp_id'] +
                "|" + rail.result('get_user_details_3')['displayText'],
                "status": "Success",
            }
        )

        statestreet_supervisorassignment_update_entry_29 = rail.WriteLogOperator(
            task_id='statestreet_supervisorassignment_update_entry_29',
            log="{{ dag_run.conf.supervisor_logtable}}",
            message="na",
            severity="",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "user_uri": dag_run.conf['user_uri'],
                "manager_id": dag_run.conf['manager_id'],
                "user_id": dag_run.conf['login_id'],
                "status": "Assigned"
            }
        )

        statestreet_userimport_logs_add_entry_31 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_31',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User Supervisor -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " No Supervisor Permission Set assigned to Manager ID" +
                    dag_run.conf['manager_id'],
                "field_name": dag_run.conf['login_id'] + "|" + dag_run.conf['emp_id'] + "|" + rail.result('get_user_details_3')['displayText'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_33 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_33',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User Supervisor -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "User with Manager ID" +
                    dag_run.conf['manager_id'] + "not found",
                "field_name": dag_run.conf['login_id'] + "|" + dag_run.conf['emp_id'] + "|" + rail.result('get_user_details_3')['displayText'],
                "status": "Exception",
            }
        )

        catch_35 = rail.EmptyOperator(
            task_id='catch_35',
            trigger_rule='one_failed',
        )

        statestreet_userimport_logs_add_entry_36 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_36',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User Supervisor -" + rail.render_template("{{dag_run_ecid()}}") + "-" + rail.render_template("{{get_error_message()}}"),
                "field_name": dag_run.conf['login_id'] + "|" + dag_run.conf['emp_id'] + "|" + rail.result('get_user_details_3')['displayText'],
                "status": "Error"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_user_details_3
        get_user_details_3 >> if_request_managerid_present_7
        if_request_managerid_present_7 >> rail.Label(
            'Yes') >> get_enabled_users_8 >> if_first_datatype_present_9
        if_first_datatype_present_9 >> rail.Label(
            'Yes') >> foreach_d_10 >> accumulate_list_items_11 >> foreach_d_10_end
        foreach_d_10 >> foreach_d_10_end >> log_supervisorcount_12 >> if_log_supervisorcount_12_greater_than_1_13
        if_log_supervisorcount_12_greater_than_1_13 >> rail.Label(
            'Yes') >> statestreet_userimport_logs_add_entry_14 >> catch_35
        if_log_supervisorcount_12_greater_than_1_13 >> rail.Label(
            'No') >> if_log_supervisorcount_12_equals_to_1_16
        if_log_supervisorcount_12_equals_to_1_16 >> rail.Label(
            'Yes') >> log_supervisor_uri_17 >> if_log_supervisor_uri_17_present_18
        if_log_supervisorcount_12_equals_to_1_16 >> rail.Label(
            'No') >> if_log_supervisor_uri_17_present_18
        if_first_datatype_present_9 >> rail.Label(
            'No') >> if_log_supervisor_uri_17_present_18
        if_log_supervisor_uri_17_present_18 >> rail.Label(
            'Yes') >> if_log_supervisor_uri_17_not_equals_to_datarestget_user_details_3responsedsupervisoruri_19
        if_log_supervisor_uri_17_not_equals_to_datarestget_user_details_3responsedsupervisoruri_19 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_20 >> log_check_supervisor_permission_21
        log_check_supervisor_permission_21 >> log_todays_date >> if_log_check_supervisor_permission_21_present_22
        if_log_check_supervisor_permission_21_present_22 >> rail.Label(
            'Yes') >> apply_user_modifications_23 >> if_d_errors_present_24
        if_d_errors_present_24 >> rail.Label(
            'Yes') >> log_errormessage_25 >> statestreet_userimport_logs_add_entry_26 >> catch_35
        if_d_errors_present_24 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_28 >> statestreet_supervisorassignment_update_entry_29 >> catch_35
        if_log_check_supervisor_permission_21_present_22 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_31 >> catch_35
        if_log_supervisor_uri_17_not_equals_to_datarestget_user_details_3responsedsupervisoruri_19 >> rail.Label(
            'No') >> catch_35
        if_request_managerid_present_7 >> rail.Label('No') >> catch_35
        if_log_supervisor_uri_17_present_18 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_33 >> catch_35 >> statestreet_userimport_logs_add_entry_36 >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)

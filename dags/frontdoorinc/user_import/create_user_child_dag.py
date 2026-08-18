
from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from frontdoorinc.user_import.utils import custom_methods

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'frontdoorinc_frontdoorinc_create_user_child_{config.instance}',
        description=f'Frontdoorinc_frontdoorinc_create_user_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
            no_task='declare_list_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='exception',
            value=[]
        )

        log_exceptionlog_4 = rail.PythonOperator(
            task_id='log_exceptionlog_4',
            python_callable=lambda dag_run: custom_methods.get_exception_log(dag_run)
        )

        search_users_5 = rail.RepliconServiceOperator(
            task_id='search_users_5',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:employee-id"
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
                            "text": dag_run.conf['employeeid'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        def get_useruri(dag_run):
            result = rail.result('search_users_5')['rows'][0] if rail.result('search_users_5') and rail.result('search_users_5')['rows'] and rail.result('search_users_5')['rows'][0] and rail.result(
                'search_users_5')['rows'][0]['cells'] and rail.result('search_users_5')['rows'][0]['cells'][0] and rail.result('search_users_5')['rows'][0]['cells'][0]['uri'] else null
            if result['cells'][3]['textValue'] == dag_run.conf['employeeid']:
                return rail.smartjoin_by_delim(result['cells'][0]['uri'], "")
            return None

        def get_enabled_user(dag_run):
            data = rail.result('search_users_5')['rows'][0] if rail.result('search_users_5') and rail.result('search_users_5')['rows'] and rail.result('search_users_5')['rows'][0] and rail.result(
                'search_users_5')['rows'][0]['cells'] and rail.result('search_users_5')['rows'][0]['cells'][0] and rail.result('search_users_5')['rows'][0]['cells'][0]['uri'] else null
            if data['cells'][3]['textValue'] == dag_run.conf['employeeid']:
                return data['cells'][2]['boolValue']
            return None

        invoke_custom_ruby_code_6 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_6',
            python_callable=lambda dag_run: {
                "user": (rail.result('search_users_5')['rows'][0]['cells'][3]['textValue'] == dag_run.conf['employeeid']) if rail.result('search_users_5') and rail.result('search_users_5')['rows'] and rail.result('search_users_5')['rows'][0] and rail.result('search_users_5')['rows'][0]['cells'] and rail.result('search_users_5')['rows'][0]['cells'][0] and rail.result('search_users_5')['rows'][0]['cells'][0]['uri'] else null,
                "useruri": get_useruri(dag_run) if rail.result('search_users_5') and rail.result('search_users_5')['rows'] and rail.result('search_users_5')['rows'][0] else null,
                "enabled": get_enabled_user(dag_run) if rail.result('search_users_5') and rail.result('search_users_5')['rows'] and rail.result('search_users_5')['rows'][0] else null
            }
        )

        if_output_user_present_7 = rail.IfOperator(
            task_id='if_output_user_present_7',
            test='''{{ result('invoke_custom_ruby_code_6').user | is_truthy }}''',
            yes_task="if_output_enabled_is_not_true_8",
            no_task="log_checkifuserexistwithsameloginname_13",
        )

        if_output_enabled_is_not_true_8 = rail.IfOperator(
            task_id='if_output_enabled_is_not_true_8',
            test='''{{ result('invoke_custom_ruby_code_6').enabled | is_falsy }}''',
            yes_task="process_update_child",
            no_task="log_checkifuserexistwithsameloginname_13",
        )

        process_update_child = rail.TriggerDagRunOperator(
            task_id='process_update_child',
            retries=0,
            trigger_dag_id=f'frontdoorinc_user_import_update_user_child_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                "lastname": "{{ dag_run.conf.lastname }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "timezone": "{{ dag_run.conf.timezone }}",
                "useruri": "{{ result('invoke_custom_ruby_code_6').useruri }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "company": "{{ dag_run.conf.company }}",
                "departmenturi": "{{ dag_run.conf.departmenturi }}",
                "hiredate": "{{ dag_run.conf.hiredate }}",
                "jobprofilecode": "{{ dag_run.conf.jobprofilecode }}",
                "timetype": "{{ dag_run.conf.timetype }}",
                "employeetypeuri": "{{ dag_run.conf.employeetypeuri }}",
                "managerid": "{{ dag_run.conf.managerid }}",
                "terminationdate": "{{ dag_run.conf.terminationdate }}",
                "emailaddress": "{{ dag_run.conf.emailaddress }}",
                "jobprofilename": "{{ dag_run.conf.jobprofilename }}",
                "costcenterid": "{{ dag_run.conf.costcenterid }}",
                "statelocation": "{{ dag_run.conf.statelocation }}",
                "locationuri": "{{ dag_run.conf.locationuri }}",
                "costcentername": "{{ dag_run.conf.costcentername }}",
                "hourlyrate": "{{ dag_run.conf.hourlyrate }}",
                "customfielduri_jobprofilename": "{{ dag_run.conf.customfielduri_jobprofilename }}",
                "customfielduri_jobprofilecode": "{{ dag_run.conf.customfielduri_jobprofilecode }}",
                "customfielduri_adminmodified": "{{ dag_run.conf.customfielduri_adminmodified }}",
                "basecurrencyuri": "{{ dag_run.conf.basecurrencyuri }}",
                "mangaerdetails": {
                    "employeeid": "{{ dag_run.conf.mangaerdetails.employeeid }}",
                    "firstname": "{{ dag_run.conf.mangaerdetails.firstname }}",
                    "lastname": "{{ dag_run.conf.mangaerdetails.lastname }}",
                    "company": "{{ dag_run.conf.mangaerdetails.company }}",
                    "departmenturi": "{{ dag_run.conf.mangaerdetails.departmenturi }}",
                    "hiredate": "{{ dag_run.conf.mangaerdetails.hiredate }}",
                    "jobprofilecode": "{{ dag_run.conf.mangaerdetails.jobprofilecode }}",
                    "timetype": "{{ dag_run.conf.mangaerdetails.timetype }}",
                    "employeetypeuri": "{{ dag_run.conf.mangaerdetails.employeetypeuri }}",
                    "timezone": "{{ dag_run.conf.mangaerdetails.timezone }}",
                    "managerid": "{{ dag_run.conf.mangaerdetails.managerid }}",
                    "terminationdate": "{{ dag_run.conf.mangaerdetails.terminationdate }}",
                    "emailaddress": "{{ dag_run.conf.mangaerdetails.emailaddress }}",
                    "jobprofilename": "{{ dag_run.conf.mangaerdetails.jobprofilename }}",
                    "costcenterid": "{{ dag_run.conf.mangaerdetails.costcenterid }}",
                    "statelocation": "{{ dag_run.conf.mangaerdetails.statelocation }}",
                    "managerlocationuri": "{{ dag_run.conf.mangaerdetails.managerlocationuri }}",
                    "costcentername": "{{ dag_run.conf.mangaerdetails.costcentername }}",
                    "hourlyrate": "{{ dag_run.conf.mangaerdetails.hourlyrate }}",
                    "customfielduri_jobprofilename": "{{ dag_run.conf.mangaerdetails.customfielduri_jobprofilename }}",
                    "customfielduri_jobprofilecode": "{{ dag_run.conf.mangaerdetails.customfielduri_jobprofilecode }}",
                    "customfielduri_adminmodified": "{{ dag_run.conf.mangaerdetails.customfielduri_adminmodified }}"
                },
                "job_id": "{{ dag_run.conf.job_id }}",
                "lookuptable": "{{ dag_run.conf.lookuptable }}"
            }
        )

        wait_for_process_update_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_child',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            dag_runs='{{ result("process_update_child") }}'
        )

        log_checkifuserexistwithsameloginname_13 = rail.PythonOperator(
            task_id='log_checkifuserexistwithsameloginname_13',
            python_callable=lambda dag_run: (rail.result('search_users_5')['rows'][0]['cells'][3]['textValue'] == dag_run.conf['employeeid']) if rail.result('search_users_5') and rail.result('search_users_5')['rows'] and rail.result(
                'search_users_5')['rows'][0] and rail.result('search_users_5')['rows'][0]['cells'] and rail.result('search_users_5')['rows'][0]['cells'][0] and rail.result('search_users_5')['rows'][0]['cells'][0]['uri'] else null,
        )

        if_log_checkifuserexistwithsameloginname_13_present_14 = rail.IfOperator(
            task_id='if_log_checkifuserexistwithsameloginname_13_present_14',
            test='''{{ result('log_checkifuserexistwithsameloginname_13') | is_truthy }}''',
            yes_task="frontdoorinc_user_import_logs_add_entry_15",
            no_task="if_request_terminationdate_present_17",
        )

        frontdoorinc_user_import_logs_add_entry_15 = rail.WriteLogOperator(
            task_id='frontdoorinc_user_import_logs_add_entry_15',
            log="{{dag_run.conf.lookuptable}}",
            message="na",
            severity="ignored",
            properties=lambda dag_run: {
                "username": str(dag_run.conf['firstname']) + " " + str(dag_run.conf['lastname']),
                "employeeid": dag_run.conf['employeeid'],
                "action": "add",
                "status": "ignored",
                "details": "A user already exist with same login name " + str(dag_run.conf['employeeid']),
                "jobid": dag_run.conf['job_id'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        if_request_terminationdate_present_17 = rail.IfOperator(
            task_id='if_request_terminationdate_present_17',
            test='''{{ dag_run.conf.terminationdate | is_truthy }}''',
            yes_task="frontdoorinc_user_import_logs_add_entry_18",
            no_task="put_user2_21",
        )

        frontdoorinc_user_import_logs_add_entry_18 = rail.WriteLogOperator(
            task_id='frontdoorinc_user_import_logs_add_entry_18',
            log="{{dag_run.conf.lookuptable}}",
            message="na",
            severity="exception",
            properties=lambda dag_run: {
                "username": str(dag_run.conf['firstname']) + " " + str(dag_run.conf['lastname']),
                "employeeid": dag_run.conf['employeeid'],
                "action": "add",
                "status": "exception",
                "details": "User not created as the termination date is already present",
                "jobid": dag_run.conf['job_id'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        put_user2_21 = rail.RepliconServiceOperator(
            task_id='put_user2_21',
            endpoint="/services/ImportService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['emailaddress'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": dag_run.conf['emailaddress'],
                    "employeeId": dag_run.conf['employeeid'],
                    "department": null,
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": "8 hours/day; Mon-Fri",
                                "officeSchedule": null,
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").year,
                            "month": datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").month,
                            "day": datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").day,
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['emailaddress'],
                        "SSOName": dag_run.conf['emailaddress'],
                        "password": null
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": [
                        {
                            "uri": null,
                            "name": "Project Resource with Reports"
                        }
                    ],
                    "policySets": [
                        {
                            "uri": null,
                            "name": "Time distribution - FTE"
                        }
                    ],
                    "employeeType": null,
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {
                        "uri": null,
                        "name": "Frontdoor Approval Path"
                    },
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": [
                        {
                            "customField": {
                                "uri": dag_run.conf['customfielduri_jobprofilecode'],
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": null,
                            "number": dag_run.conf['jobprofilecode']
                        }
                    ],
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "departmentGroupSchedule": [],
                    "employeeTypeGroupSchedule": [],
                    "timesheetPeriodSchedule": [
                        {
                            "timesheetPeriod": {
                                "uri": null,
                                "name": "Monthly"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": [],
                    "displayNameParameter": null
                }
            }
        )

        if_request_departmenturi_present_22 = rail.IfOperator(
            task_id='if_request_departmenturi_present_22',
            test='''{{ dag_run.conf.departmenturi | is_truthy }}''',
            yes_task="put_department_group_schedule_for_user_23",
            no_task="if_request_timezone_present_24",
        )

        put_department_group_schedule_for_user_23 = rail.RepliconServiceOperator(
            task_id='put_department_group_schedule_for_user_23',
            endpoint="/services/DepartmentGroupService1.svc/PutDepartmentGroupScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_21').uri }}",
                "scheduleEntries": [
                    {
                        "departmentGroup": {
                            "uri": "{{ dag_run.conf.departmenturi }}",
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_request_timezone_present_24 = rail.IfOperator(
            task_id='if_request_timezone_present_24',
            test='''{{ dag_run.conf.timezone | is_truthy }}''',
            yes_task="apply_user_modifications2_timezone_25",
            no_task="if_request_employeetypeuri_present_26",
        )

        apply_user_modifications2_timezone_25 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_timezone_25',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('put_user2_21').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": {
                        "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                        "timezone": {
                            "uri": null,
                            "IANAName": "{{ dag_run.conf.timezone }}"
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }

        )

        if_request_employeetypeuri_present_26 = rail.IfOperator(
            task_id='if_request_employeetypeuri_present_26',
            test='''{{ dag_run.conf.employeetypeuri | is_truthy }}''',
            yes_task="apply_user_modifications2employee_type_group_27",
            no_task="if_request_locationuri_present_28",
        )

        apply_user_modifications2employee_type_group_27 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2employee_type_group_27',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('put_user2_21').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications":  {
                    "employeeTypeGroupScheduleToApply": {
                        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                        "replacementEmployeeTypeGroupSchedule": [
                            {
                                "employeeTypeGroup": {
                                    "uri": "{{ dag_run.conf.employeetypeuri }}",
                                    "parent": null,
                                    "name": null,
                                    "parameterCorrelationId": null
                                },
                                "effectiveDate": null
                            }
                        ],
                        "updateEmployeeTypeGroupScheduleOverDateRange": null
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_locationuri_present_28 = rail.IfOperator(
            task_id='if_request_locationuri_present_28',
            test='''{{ dag_run.conf.locationuri | is_truthy }}''',
            yes_task="apply_user_modifications2location_29",
            no_task="if_request_costcenterid_present_30",
        )

        apply_user_modifications2location_29 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2location_29',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('put_user2_21').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications":  {
                    "locationScheduleToApply": {
                        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                        "replacementLocationSchedule": [
                            {
                                "location": {
                                    "uri": "{{ dag_run.conf.locationuri }}",
                                    "parentUri": null,
                                    "name": null
                                },
                                "effectiveDate": null
                            }
                        ],
                        "updateLocationScheduleOverDateRange": null
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_costcenterid_present_30 = rail.IfOperator(
            task_id='if_request_costcenterid_present_30',
            test='''{{ dag_run.conf.costcenterid | is_truthy }}''',
            yes_task="apply_user_modifications2service_center_schedule_31",
            no_task="if_request_jobprofilename_present_32",
        )

        apply_user_modifications2service_center_schedule_31 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2service_center_schedule_31',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('put_user2_21').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications":  {
                    "costCenterScheduleToApply": {
                        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                        "replacementCostCenterSchedule": [
                            {
                                "costCenter": {
                                    "uri": "{{ dag_run.conf.costcenterid }}",
                                    "parentUri": null,
                                    "name": null
                                },
                                "effectiveDate": null
                            }
                        ],
                        "updateCostCenterScheduleOverDateRange": null
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_jobprofilename_present_32 = rail.IfOperator(
            task_id='if_request_jobprofilename_present_32',
            test='''{{ dag_run.conf.jobprofilename | is_truthy  and dag_run.conf.customfielduri_jobprofilecode | is_truthy }}''',
            yes_task="update_text_value_jobprofilename_33",
            no_task="if_request_hourlyrate_present_34",
        )

        update_text_value_jobprofilename_33 = rail.RepliconServiceOperator(
            task_id='update_text_value_jobprofilename_33',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2_21').uri }}",
                "customFieldUri": "{{ dag_run.conf.customfielduri_jobprofilename }}",
                "value": "{{ dag_run.conf.jobprofilename }}"
            }
        )

        if_request_hourlyrate_present_34 = rail.IfOperator(
            task_id='if_request_hourlyrate_present_34',
            test='''{{ dag_run.conf.hourlyrate | is_truthy }}''',
            yes_task="put_user_payroll_rate_schedule_35",
            no_task="if_request_managerid_present_38",
        )

        put_user_payroll_rate_schedule_35 = rail.RepliconServiceOperator(
            task_id='put_user_payroll_rate_schedule_35',
            endpoint="/services/ResourceService1.svc/PutUserCostRateSchedule",
            data={
                "userUri": "{{ result('put_user2_21').uri }}",
                "schedule": {
                    "initialHourlyRate": {
                        "amount": "{{ dag_run.conf.hourlyrate }}",
                        "currency": {
                            "uri": null,
                            "name": null,
                            "symbol": null
                        }
                    },
                    "scheduleEntries": []
                }
            }
        )

        if_request_managerid_present_38 = rail.IfOperator(
            task_id='if_request_managerid_present_38',
            test='''{{ dag_run.conf.managerid | is_truthy }}''',
            yes_task="search_users_39",
            no_task="if_log_exceptionlog_4_present_70",
        )

        search_users_39 = rail.RepliconServiceOperator(
            task_id='search_users_39',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:employee-id"
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
                            "text": dag_run.conf['managerid'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        def get_uri(dag_run):
            result = rail.result('search_users_39')['rows'][0] if rail.result('search_users_39') and rail.result('search_users_39')['rows'] and rail.result('search_users_39')['rows'][0] and rail.result(
                'search_users_39')['rows'][0]['cells'] and rail.result('search_users_39')['rows'][0]['cells'][0] and rail.result('search_users_39')['rows'][0]['cells'][0]['uri'] else null
            if result['cells'][3]['textValue'] == dag_run.conf['managerid']:
                return rail.smartjoin_by_delim(result['cells'][0]['uri'], "")
            return None

        log_checkifuserexist_40 = rail.PythonOperator(
            task_id='log_checkifuserexist_40',
            python_callable=lambda dag_run: get_uri(dag_run) if rail.result(
                'search_users_39') and rail.result('search_users_39')['rows'] and rail.result('search_users_39')['rows'][0] else null
        )

        if_log_checkifuserexist_40_present_41 = rail.IfOperator(
            task_id='if_log_checkifuserexist_40_present_41',
            test='''{{ result('log_checkifuserexist_40') | is_truthy }}''',
            yes_task="if_enabled_boolvalue_is_true_42",
            no_task="if_mangaerdetails_employeeid_present_55",
        )

        if_enabled_boolvalue_is_true_42 = rail.IfOperator(
            task_id='if_enabled_boolvalue_is_true_42',
            test="{{result('search_users_39').rows[0].cells[2].boolValue | is_truthy}}",
            yes_task="get_assigned_permission_sets_for_user2_43",
            no_task="insert_to_list_53",
        )

        get_assigned_permission_sets_for_user2_43 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_43',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('log_checkifuserexist_40') }}"
            }
        )

        def get_permissionset():
            record = rail.result('get_assigned_permission_sets_for_user2_43') if rail.result(
                'get_assigned_permission_sets_for_user2_43') else null
            for d in record:
                if d['permissionSet']['name'] == "Supervisor":
                    return d['permissionSet']
            return None

        log_checkifsupervisorpermissionsetisassigned_44 = rail.PythonOperator(
            task_id='log_checkifsupervisorpermissionsetisassigned_44',
            python_callable=get_permissionset
        )

        if_log_checkifsupervisorpermissionsetisassigned_44_present_45 = rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionsetisassigned_44_present_45',
            test='''{{ result('log_checkifsupervisorpermissionsetisassigned_44') | is_truthy }}''',
            yes_task="put_supervisor_assignment_schedule_46",
            no_task="if_log_checkifsupervisorpermissionsetisassigned_44_blank_47",
        )

        put_supervisor_assignment_schedule_46 = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule_46',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ result('put_user2_21').uri }}",
                "initialSupervisorUri": "{{ result('log_checkifuserexist_40') }}",
                "scheduleEntries": []
            }
        )

        if_log_checkifsupervisorpermissionsetisassigned_44_blank_47 = rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionsetisassigned_44_blank_47',
            test='''{{ result('log_checkifsupervisorpermissionsetisassigned_44') | is_falsy }}''',
            yes_task="get_all_permission_sets_48",
            no_task="insert_to_list_53",
        )

        get_all_permission_sets_48 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_48',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        log_get_supervisorpermissionuri_49 = rail.PythonOperator(
            task_id='log_get_supervisorpermissionuri_49',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permission_sets_48'), 'name', 'Supervisor', 'uri', "")
        )

        assign_permission_set_to_user_50 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_50',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('log_checkifuserexist_40') }}",
                "permissionSetUri": "{{ result('log_get_supervisorpermissionuri_49') }}"
            }
        )

        put_supervisor_assignment_schedule_51 = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule_51',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ result('put_user2_21').uri }}",
                "initialSupervisorUri": "{{ result('log_checkifuserexist_40') }}",
                "scheduleEntries": []
            }
        )

        insert_to_list_53 = rail.SetVariableOperator(
            task_id='insert_to_list_53',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Supervisor was not assigned as the required Supervisor is disabled in Replicon."
            }
        )

        if_mangaerdetails_employeeid_present_55 = rail.IfOperator(
            task_id='if_mangaerdetails_employeeid_present_55',
            test='''{{ dag_run.conf.mangaerdetails.employeeid | is_truthy }}''',
            yes_task="process_supervisor_child",
            no_task="if_log_exceptionlog_4_present_70",
        )

        process_supervisor_child = rail.TriggerDagRunOperator(
            task_id='process_supervisor_child',
            retries=0,
            trigger_dag_id=f'frontdoorinc_user_import_create_supervisor_child_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                "lastname": "{{ dag_run.conf.mangaerdetails.lastname }}",
                "firstname": "{{ dag_run.conf.mangaerdetails.firstname }}",
                "timezone": "{{ dag_run.conf.mangaerdetails.timezone }}",
                "employeeid": "{{ dag_run.conf.mangaerdetails.employeeid }}",
                "company": "{{ dag_run.conf.mangaerdetails.company }}",
                "departmenturi": "{{ dag_run.conf.mangaerdetails.departmenturi }}",
                "hiredate": "{{ dag_run.conf.mangaerdetails.hiredate }}",
                "jobprofilecode": "{{ dag_run.conf.mangaerdetails.jobprofilecode }}",
                "timetype": "{{ dag_run.conf.mangaerdetails.timetype }}",
                "employeetypeuri": "{{ dag_run.conf.mangaerdetails.employeetypeuri }}",
                "managerid": "{{ dag_run.conf.mangaerdetails.managerid }}",
                "terminationdate": "{{ dag_run.conf.mangaerdetails.terminationdate }}",
                "emailaddress": "{{ dag_run.conf.mangaerdetails.emailaddress }}",
                "jobprofilename": "{{ dag_run.conf.mangaerdetails.jobprofilename }}",
                "costcenterid": "{{ dag_run.conf.mangaerdetails.costcenterid }}",
                "statelocation": "{{ dag_run.conf.mangaerdetails.statelocation }}",
                "locationuri": "{{ dag_run.conf.mangaerdetails.managerlocationuri }}",
                "costcentername": "{{ dag_run.conf.mangaerdetails.costcentername }}",
                "hourlyrate": "{{ dag_run.conf.mangaerdetails.hourlyrate }}",
                "customfielduri_jobprofilename": "{{ dag_run.conf.mangaerdetails.customfielduri_jobprofilename }}",
                "customfielduri_jobprofilecode": "{{ dag_run.conf.mangaerdetails.customfielduri_jobprofilecode }}",
                "customfielduri_adminmodified": "{{ dag_run.conf.mangaerdetails.customfielduri_adminmodified }}",
                "jobid": "{{dag_run.conf.job_id}}",
                "basecurrencyuri": "{{ dag_run.conf.basecurrencyuri }}",
                "lookuptable": "{{dag_run.conf.lookuptable}}"

            }
        )

        wait_for_process_supervisor_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_supervisor_child',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            dag_runs='{{result("process_supervisor_child")}}'
        )

        gather_list1 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_list1',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_supervisor_child') }}",
            dagrun_task_id='get_supervisor_uri',
            flatten=True
        )

        gather_list2 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_list2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_supervisor_child') }}",
            dagrun_task_id='get_supervisoruri',
            flatten=True
        )

        get_data = rail.PythonOperator(
            task_id='get_data',
            python_callable=lambda: rail.smartjoin_by_delim(rail.result(
                'gather_list1'), "") or rail.smartjoin_by_delim(rail.result('gather_list2'), "")
        )

        if_response_has_data_present = rail.IfOperator(
            task_id='if_response_has_data_present',
            test="{{result('get_data') |is_truthy }}",
            yes_task="get_assigned_permissionsets",
            no_task="insert_item_to_list",
        )

        get_assigned_permissionsets = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionsets',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('get_data') }}"
            }
        )

        def get_permissionset1():
            record = rail.result('get_assigned_permissionsets') if rail.result(
                'get_assigned_permissionsets') else null
            for d in record:
                if d['permissionSet']['name'] == "Supervisor":
                    return d['permissionSet']
            return None

        check_if_supervisorpermissionsetisassigned = rail.PythonOperator(
            task_id='check_if_supervisorpermissionsetisassigned',
            python_callable=get_permissionset1
        )

        if_check_if_supervisorpermissionsetisassigned = rail.IfOperator(
            task_id='if_check_if_supervisorpermissionsetisassigned',
            test='''{{ result('check_if_supervisorpermissionsetisassigned') | is_truthy }}''',
            yes_task="put_supervisor_assignment",
            no_task="if_check_if_supervisorpermissionsetisassigned_blank",
        )

        put_supervisor_assignment = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ result('put_user2_21').uri }}",
                "initialSupervisorUri": "{{ result('get_data') }}",
                "scheduleEntries": []
            }
        )

        if_check_if_supervisorpermissionsetisassigned_blank = rail.IfOperator(
            task_id='if_check_if_supervisorpermissionsetisassigned_blank',
            test='''{{ result('check_if_supervisorpermissionsetisassigned') | is_falsy }}''',
            yes_task="get_all_permissionsets",
            no_task="if_log_exceptionlog_4_present_70",
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_supervisorpermissionuri = rail.PythonOperator(
            task_id='get_supervisorpermissionuri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permissionsets'), 'name', 'Supervisor', 'uri', "")
        )

        assign_permission_set_to_user = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('get_data') }}",
                "permissionSetUri": "{{ result('get_supervisorpermissionuri') }}"
            }
        )

        put_supervisorassignment_schedule = rail.RepliconServiceOperator(
            task_id='put_supervisorassignment_schedule',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ result('put_user2_21').uri }}",
                "initialSupervisorUri": "{{ result('get_data') }}",
                "scheduleEntries": []
            }
        )

        insert_item_to_list = rail.SetVariableOperator(
            task_id='insert_item_to_list',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Supervisor assignment not completed as the required supervisor could not be created."
            }
        )

        if_log_exceptionlog_4_present_70 = rail.IfOperator(
            task_id='if_log_exceptionlog_4_present_70',
            test='''{{ result('log_exceptionlog_4') | is_truthy  or result('declare_list_3').value | length > 0 }}''',
            yes_task="frontdoorinc_user_import_logs_add_entry_71",
            no_task="add_user_creation_entry",
        )

        frontdoorinc_user_import_logs_add_entry_71 = rail.WriteLogOperator(
            task_id='frontdoorinc_user_import_logs_add_entry_71',
            log="{{dag_run.conf.lookuptable}}",
            message="na",
            severity="exception",
            properties=lambda dag_run: {
                "username": str(dag_run.conf['firstname']) + " " + str(dag_run.conf['lastname']),
                "employeeid": dag_run.conf['employeeid'],
                "action": "add",
                "status": "exception",
                "details": "User created successfully. " + rail.result('log_exceptionlog_4'),
                "jobid": dag_run.conf['job_id'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        stop_72 = rail.EmptyOperator(
            task_id='stop_72',
        )

        add_user_creation_entry = rail.WriteLogOperator(
            task_id='add_user_creation_entry',
            log="{{dag_run.conf.lookuptable}}",
            message="na",
            severity="success",
            properties=lambda dag_run: {
                "username": str(dag_run.conf['firstname']) + " " + str(dag_run.conf['lastname']),
                "employeeid": dag_run.conf['employeeid'],
                "action": "add",
                "status": "success",
                "details": "User created successfully.",
                "jobid": dag_run.conf['job_id'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        catch_73 = rail.EmptyOperator(
            task_id='catch_73',
            trigger_rule='one_failed',
        )

        frontdoorinc_user_import_logs_add_entry_74 = rail.WriteLogOperator(
            task_id='frontdoorinc_user_import_logs_add_entry_74',
            log="{{dag_run.conf.lookuptable}}",
            message="na",
            severity="failed",
            properties=lambda dag_run: {
                "username": str(dag_run.conf['firstname']) + " " + str(dag_run.conf['lastname']),
                "employeeid": dag_run.conf['employeeid'],
                "action": "add",
                "status": "failed",
                "details": rail.render_template("{{ get_error_message() }}"),
                "jobid": dag_run.conf['job_id'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> declare_list_3
        declare_list_3 >> log_exceptionlog_4 >> search_users_5 >> invoke_custom_ruby_code_6 >> if_output_user_present_7
        if_output_user_present_7 >> rail.Label(
            'Yes') >> if_output_enabled_is_not_true_8
        if_output_enabled_is_not_true_8 >> rail.Label(
            'Yes') >> process_update_child >> wait_for_process_update_child >> log_to_sumo
        if_output_enabled_is_not_true_8 >> rail.Label('No') >> log_checkifuserexistwithsameloginname_13
        if_output_user_present_7 >> rail.Label(
            'No') >> log_checkifuserexistwithsameloginname_13
        log_checkifuserexistwithsameloginname_13 >> if_log_checkifuserexistwithsameloginname_13_present_14
        if_log_checkifuserexistwithsameloginname_13_present_14 >> rail.Label(
            'Yes') >> frontdoorinc_user_import_logs_add_entry_15 >> log_to_sumo
        if_log_checkifuserexistwithsameloginname_13_present_14 >> rail.Label(
            'No') >> if_request_terminationdate_present_17
        if_request_terminationdate_present_17 >> rail.Label(
            'Yes') >> frontdoorinc_user_import_logs_add_entry_18 >> log_to_sumo
        if_request_terminationdate_present_17 >> rail.Label(
            'No') >> put_user2_21 >> if_request_departmenturi_present_22
        if_request_departmenturi_present_22 >> rail.Label(
            'Yes') >> put_department_group_schedule_for_user_23 >> if_request_timezone_present_24
        if_request_departmenturi_present_22 >> rail.Label(
            'No') >> if_request_timezone_present_24
        if_request_timezone_present_24 >> rail.Label(
            'Yes') >> apply_user_modifications2_timezone_25 >> if_request_employeetypeuri_present_26
        if_request_timezone_present_24 >> rail.Label(
            'No') >> if_request_employeetypeuri_present_26
        if_request_employeetypeuri_present_26 >> rail.Label(
            'Yes') >> apply_user_modifications2employee_type_group_27 >> if_request_locationuri_present_28
        if_request_employeetypeuri_present_26 >> rail.Label(
            'No') >> if_request_locationuri_present_28
        if_request_locationuri_present_28 >> rail.Label(
            'Yes') >> apply_user_modifications2location_29 >> if_request_costcenterid_present_30
        if_request_locationuri_present_28 >> rail.Label(
            'No') >> if_request_costcenterid_present_30
        if_request_costcenterid_present_30 >> rail.Label(
            'Yes') >> apply_user_modifications2service_center_schedule_31 >> if_request_jobprofilename_present_32
        if_request_costcenterid_present_30 >> rail.Label(
            'No') >> if_request_jobprofilename_present_32
        if_request_jobprofilename_present_32 >> rail.Label(
            'Yes') >> update_text_value_jobprofilename_33 >> if_request_hourlyrate_present_34
        if_request_jobprofilename_present_32 >> rail.Label(
            'No') >> if_request_hourlyrate_present_34
        if_request_hourlyrate_present_34 >> rail.Label(
            'Yes') >> put_user_payroll_rate_schedule_35 >> if_request_managerid_present_38
        if_request_hourlyrate_present_34 >> rail.Label(
            'No') >> if_request_managerid_present_38
        if_request_managerid_present_38 >> rail.Label(
            'Yes') >> search_users_39 >> log_checkifuserexist_40
        log_checkifuserexist_40 >> if_log_checkifuserexist_40_present_41
        if_log_checkifuserexist_40_present_41 >> rail.Label(
            'Yes') >> if_enabled_boolvalue_is_true_42
        if_enabled_boolvalue_is_true_42 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_43 >> log_checkifsupervisorpermissionsetisassigned_44
        log_checkifsupervisorpermissionsetisassigned_44 >> if_log_checkifsupervisorpermissionsetisassigned_44_present_45
        if_log_checkifsupervisorpermissionsetisassigned_44_present_45 >> rail.Label(
            'Yes') >> put_supervisor_assignment_schedule_46 >> if_log_checkifsupervisorpermissionsetisassigned_44_blank_47
        if_log_checkifsupervisorpermissionsetisassigned_44_present_45 >> rail.Label(
            'No') >> if_log_checkifsupervisorpermissionsetisassigned_44_blank_47
        if_log_checkifsupervisorpermissionsetisassigned_44_blank_47 >> rail.Label(
            'Yes') >> get_all_permission_sets_48 >> log_get_supervisorpermissionuri_49 >> assign_permission_set_to_user_50
        assign_permission_set_to_user_50 >> put_supervisor_assignment_schedule_51 >> insert_to_list_53
        if_log_checkifsupervisorpermissionsetisassigned_44_blank_47 >> rail.Label(
            'No') >> insert_to_list_53 >> if_log_exceptionlog_4_present_70
        if_log_checkifuserexist_40_present_41 >> rail.Label(
            'No') >> if_mangaerdetails_employeeid_present_55
        if_mangaerdetails_employeeid_present_55 >> rail.Label(
            'Yes') >> process_supervisor_child >> wait_for_process_supervisor_child >> gather_list1
        gather_list1 >> gather_list2 >> get_data >> if_response_has_data_present
        if_response_has_data_present >> rail.Label(
            'Yes') >> get_assigned_permissionsets >> check_if_supervisorpermissionsetisassigned
        check_if_supervisorpermissionsetisassigned >> if_check_if_supervisorpermissionsetisassigned
        if_check_if_supervisorpermissionsetisassigned >> rail.Label(
            'Yes') >> put_supervisor_assignment >> if_check_if_supervisorpermissionsetisassigned_blank
        if_check_if_supervisorpermissionsetisassigned_blank >> rail.Label(
            'Yes') >> get_all_permissionsets >> get_supervisorpermissionuri
        get_supervisorpermissionuri >> assign_permission_set_to_user >> put_supervisorassignment_schedule
        put_supervisorassignment_schedule >> if_log_exceptionlog_4_present_70

        if_check_if_supervisorpermissionsetisassigned >> rail.Label(
            'No') >> if_check_if_supervisorpermissionsetisassigned_blank
        if_check_if_supervisorpermissionsetisassigned_blank >> rail.Label(
            'No') >> if_log_exceptionlog_4_present_70

        if_response_has_data_present >> rail.Label(
            'No') >> insert_item_to_list >> if_log_exceptionlog_4_present_70

        if_mangaerdetails_employeeid_present_55 >> rail.Label(
            'No') >> if_log_exceptionlog_4_present_70
        if_request_managerid_present_38 >> rail.Label(
            'No') >> if_log_exceptionlog_4_present_70
        if_enabled_boolvalue_is_true_42 >> rail.Label(
            'No') >> insert_to_list_53
        if_log_exceptionlog_4_present_70 >> rail.Label(
            'Yes') >> frontdoorinc_user_import_logs_add_entry_71 >> stop_72 >> log_to_sumo
        if_log_exceptionlog_4_present_70 >> rail.Label(
            'No') >> add_user_creation_entry >> catch_73 >> frontdoorinc_user_import_logs_add_entry_74 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)

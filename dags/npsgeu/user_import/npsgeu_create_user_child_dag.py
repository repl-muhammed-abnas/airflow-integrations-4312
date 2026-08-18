
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'npsgeu_user_import_create_user_child_{config.instance}',
        description=f'NPSGEU_create_user {config.instance}',
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_list_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='exception',
            value=[]
        )

        check_for_exceptions = rail.PythonOperator(
            task_id='check_for_exceptions',
            python_callable=lambda dag_run: rail.smartjoin_by_delim(
                ((("" if dag_run.conf['locationuri'] else "Location provided is not available in Replicon") if dag_run.conf['location'] else (
                    "Location not assigned as it is blank in feedfile")) + " " +
                    ("" if dag_run.conf['timezone'] else "Timezone recieved is not available in Replicon") + " " + (
                    "" if dag_run.conf['punchtimenetry'] else "Punch entry policy recieved is blank/not available in Replicon") + " " +
                    ("" if dag_run.conf['timesheettemplate'] else "Timesheet template recieved is blank/not available in Replicon") + " " +
                    ("" if dag_run.conf['timeofftemplate'] else "Timeoff template recieved is blank/not available in Replicon") + " " +
                 ("" if dag_run.conf['payrule'] else "Payrule recieved is blank/not available in Replicon")).split(","), ",")
        )

        if_log_exceptionlog_8_present_9 = rail.IfOperator(
            task_id='if_log_exceptionlog_8_present_9',
            test='''{{ result('check_for_exceptions') | is_truthy }}''',
            yes_task="insert_to_list_10",
            no_task="search_users_11",
        )

        insert_to_list_10 = rail.SetVariableOperator(
            task_id='insert_to_list_10',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "{{result('check_for_exceptions')}}"
            }
        )

        def get_user_details(response):
            required_user = response[0] if response else []
            return {
                'user': required_user if required_user else '',
                'useruri': required_user['userDetails']['uri'] if required_user else '',
                'enabled': required_user['userDetails']['isEnabled'] if required_user else '',
            }

        search_users_11 = rail.RepliconServiceOperator(
            task_id='search_users_11',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                    "uri": null,
                    "loginName": null,
                    "employeeId": "{{dag_run.conf.employeeid}}",
                    "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=get_user_details
        )

        if_output_user_present_13 = rail.IfOperator(
            task_id='if_output_user_present_13',
            test='''{{ result('search_users_11').user | is_truthy }}''',
            yes_task="if_output_enabled_is_not_true_14",
            no_task="search_user_by_loginname",
        )

        if_output_enabled_is_not_true_14 = rail.IfOperator(
            task_id='if_output_enabled_is_not_true_14',
            test='''{{ result('search_users_11').enabled | is_falsy }}''',
            yes_task="trigger_dag_run_live_npsgeu_update_user_v1_015",
            no_task="search_user_by_loginname",
        )

        trigger_dag_run_live_npsgeu_update_user_v1_015 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_npsgeu_update_user_v1_015',
            retries=0,
            trigger_dag_id=f'npsgeu_user_import_update_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "email": "{{ dag_run.conf.email }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "enddate": "{{ dag_run.conf.enddate }}",
                "employmentstatus": "{{ dag_run.conf.employmentstatus }}",
                "division": "{{ dag_run.conf.division }}",
                "position": "{{ dag_run.conf.position }}",
                "employeestate": "{{ dag_run.conf.employeestate }}",
                "employeecity": "{{ dag_run.conf.employeecity }}",
                "loginanme": "{{ dag_run.conf.loginanme }}",
                "supervisorid": "{{ dag_run.conf.supervisorid }}",
                "department": "{{ dag_run.conf.department }}",
                "departmenturi": "{{ dag_run.conf.departmenturi }}",
                "employeetype": "{{ dag_run.conf.employeetype }}",
                "employeetypeuri": "{{ dag_run.conf.employeetypeuri }}",
                "loginenabled": "{{ dag_run.conf.loginenabled }}",
                "license": "{{ dag_run.conf.license }}",
                "jobfamily": "{{ dag_run.conf.jobfamily }}",
                "managementlevel": "{{ dag_run.conf.managementlevel }}",
                "location": "{{ dag_run.conf.location }}",
                "locationuri": "{{ dag_run.conf.locationuri }}",
                "punchtimenetry": "{{ dag_run.conf.punchtimenetry }}",
                "timesheettemplate": "{{ dag_run.conf.timesheettemplate }}",
                "timeofftemplate": "{{ dag_run.conf.timeofftemplate }}",
                "timezone": "{{ dag_run.conf.timezone }}",
                "holidaycalendar": "{{ dag_run.conf.holidaycalendar }}",
                "payrule": "{{ dag_run.conf.payrule }}",
                "udfuri_division": "{{ dag_run.conf.udfuri_division }}",
                "udfuri_position": "{{ dag_run.conf.udfuri_position }}",
                "udfuri_employeestate": "{{ dag_run.conf.udfuri_employeestate }}",
                "udfuri_employeecity": "{{ dag_run.conf.udfuri_employeecity }}",
                "udfuri_employementstatus": "{{ dag_run.conf.udfuri_employementstatus }}",
                "permissions": "{{ dag_run.conf.permissions }}",
                "useruri": "{{ result('search_users_11').useruri }}",
                "masterjob": "{{dag_run.conf.callerjobid}}",
                "divisionuri": "{{ dag_run.conf.divisionuri }}",
                "positionuri": "{{ dag_run.conf.positionuri }}",
                "employeestateuri": "{{ dag_run.conf.employeestateuri }}",
                "startdate_DS": {
                    "day": "{{ dag_run.conf.startdate_DS.day }}",
                    "month": "{{ dag_run.conf.startdate_DS.month }}",
                    "year": "{{ dag_run.conf.startdate_DS.year }}"
                },
                "enddate_DS": {
                    "month": "{{ dag_run.conf.enddate_DS.month }}",
                    "day": "{{ dag_run.conf.enddate_DS.day }}",
                    "year": "{{ dag_run.conf.enddate_DS.year }}"
                },
                "today_DS": {
                    "day": "{{ dag_run.conf.today_DS.day }}",
                    "month": "{{ dag_run.conf.today_DS.month }}",
                    "year": "{{ dag_run.conf.today_DS.year }}"
                },
                "additionalroles": "{{ dag_run.conf.additionalroles }}",
                "primaryroles": "{{ dag_run.conf.primaryroles }}",
                "effective_DS": {
                    "day": "{{ dag_run.conf.effective_DS.day }}",
                    "month": "{{ dag_run.conf.effective_DS.month }}",
                    "year": "{{ dag_run.conf.effective_DS.year }}"
                },
                "callerjobid": "{{dag_run.conf.callerjobid}}",
                "userimportlogtable": "{{dag_run.conf.userimportlogtable}}",
                "supervisorlookup": "{{dag_run.conf.supervisorlookup}}"
            }
        )

        wait_for_completion_trigger_dag_run_live_npsgeu_update_user_v1_015 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_npsgeu_update_user_v1_015',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_npsgeu_update_user_v1_015") }}'
        )

        search_user_by_loginname = rail.RepliconServiceOperator(
            task_id = 'search_user_by_loginname',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                    "uri": null,
                    "loginName": "{{dag_run.conf.loginanme}}",
                    "employeeId": null,
                    "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
        )

        if_log_checkifuserexistwithsameloginname_19_present_20 = rail.IfOperator(
            task_id='if_log_checkifuserexistwithsameloginname_19_present_20',
            test=lambda: rail.result('search_user_by_loginname') and rail.result('search_user_by_loginname')[0],
            yes_task="npsg_user_import_logs_add_entry_21",
            no_task="if_request_enddate_present_23",
        )

        npsg_user_import_logs_add_entry_21 = rail.WriteLogOperator(
            task_id='npsg_user_import_logs_add_entry_21',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="exception",
            properties={
                "empid": "{{dag_run.conf.employeeid}}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "add",
                "status": "exception",
                "details": "A user already exist with same login name {{ dag_run.conf.loginanme }}.",
                "parentjob": "{{dag_run.conf.callerjobid}}",
                "childjob": "{{ dag_run_ecid() }}"
            }
        )

        if_request_enddate_present_23 = rail.IfOperator(
            task_id='if_request_enddate_present_23',
            test='''{{ dag_run.conf.enddate | is_truthy }}''',
            yes_task="npsg_user_import_logs_add_entry_24",
            no_task="put_user2_27",
        )

        npsg_user_import_logs_add_entry_24 = rail.WriteLogOperator(
            task_id='npsg_user_import_logs_add_entry_24',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="exception",
            properties={
                "empid": "{{dag_run.conf.employeeid}}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "add",
                "status": "exception",
                "details": "User not created as the End date is already present",
                "childjob": "{{ dag_run_ecid() }}",
                "parentjob": "{{dag_run.conf.callerjobid}}"
            }
        )

        put_user2_27 = rail.RepliconServiceOperator(
            task_id='put_user2_27',
            endpoint="/services/ImportService1.svc/PutUser2",
            data={
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": "{{ dag_run.conf.employeeid }}",
                        "parameterCorrelationId": null
                    },
                    "firstname": "{{ dag_run.conf.firstname }}",
                    "lastname": "{{ dag_run.conf.lastname }}",
                    "emailAddress": "{{ dag_run.conf.email }}",
                    "employeeId": "{{ dag_run.conf.employeeid }}",
                    "department": {
                        "uri": "{{ dag_run.conf.departmenturi }}",
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": "8 hours/day; All Days (Hourly)",
                                "officeSchedule": null,
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": "{{ dag_run.conf.startdate_DS.year }}",
                            "month": "{{ dag_run.conf.startdate_DS.month }}",
                            "day": "{{ dag_run.conf.startdate_DS.day }}"
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:replicon"
                        ],
                        "isLoginEnabled": "true",
                        "loginName": "{{ dag_run.conf.employeeid }}",
                        "SSOName": null,
                        "password": "Npsg2020@"
                    },
                    "holidayCalendar": {
                        "uri": null,
                        "name": "{{ dag_run.conf.holidaycalendar }}"
                    },
                    "timeOffPolicy": null,
                    "permissionSets": [
                        {
                            "uri": null,
                            "name": "Basic User"
                        }
                    ],
                    "policySets": [],
                    "employeeType": {
                        "uri": null,
                        "name": "{{ dag_run.conf.employeetype }}"
                    },
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": [],
                    "assignedActivities": [],
                    "timeZone": {
                        "uri": "urn:replicon:time-zone:america-indianapolis",
                        "IANAName": null
                    },
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "departmentGroupSchedule": [],
                    "employeeTypeGroupSchedule": [],
                    "timesheetPeriodSchedule": [],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": [],
                    "displayNameParameter": null
                }
            }
        )

        if_request_locationuri_present_28 = rail.IfOperator(
            task_id='if_request_locationuri_present_28',
            test='''{{ dag_run.conf.locationuri | is_truthy }}''',
            yes_task="apply_user_modifications2location_29",
            no_task="if_request_employmentstatus_present_30",
        )

        apply_user_modifications2location_29 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2location_29',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('put_user2_27').uri }}",
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

        if_request_employmentstatus_present_30 = rail.IfOperator(
            task_id='if_request_employmentstatus_present_30',
            test='''{{ dag_run.conf.employmentstatus | is_truthy }}''',
            yes_task="update_dropdown_value_employementstatus_31",
            no_task="if_request_division_present_32",
        )

        update_dropdown_value_employementstatus_31 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_employementstatus_31',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_27').uri }}",
                "customFieldUri": "{{ dag_run.conf.udfuri_employementstatus }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.employmentstatus }}"
            }
        )

        if_request_division_present_32 = rail.IfOperator(
            task_id='if_request_division_present_32',
            test='''{{ dag_run.conf.division | is_truthy }}''',
            yes_task="update_dropdown_value_division_33",
            no_task="if_request_position_present_34",
        )

        update_dropdown_value_division_33 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_division_33',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_27').uri }}",
                "customFieldUri": "{{ dag_run.conf.udfuri_division }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.divisionuri }}"
            }
        )

        if_request_position_present_34 = rail.IfOperator(
            task_id='if_request_position_present_34',
            test='''{{ dag_run.conf.position | is_truthy }}''',
            yes_task="update_dropdown_value_position_35",
            no_task="if_request_employeestate_present_36",
        )

        update_dropdown_value_position_35 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_position_35',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_27').uri }}",
                "customFieldUri": "{{ dag_run.conf.udfuri_position }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.positionuri }}"
            }
        )

        if_request_employeestate_present_36 = rail.IfOperator(
            task_id='if_request_employeestate_present_36',
            test='''{{ dag_run.conf.employeestate | is_truthy }}''',
            yes_task="update_text_value_employeestate_37",
            no_task="if_request_employeecity_present_38",
        )

        update_text_value_employeestate_37 = rail.RepliconServiceOperator(
            task_id='update_text_value_employeestate_37',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2_27').uri }}",
                "customFieldUri": "{{ dag_run.conf.udfuri_employeestate }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.employeestateuri }}"
            }
        )

        if_request_employeecity_present_38 = rail.IfOperator(
            task_id='if_request_employeecity_present_38',
            test='''{{ dag_run.conf.employeecity | is_truthy }}''',
            yes_task="update_text_value_employeecity_39",
            no_task="if_request_punchtimenetry_present_40",
        )

        update_text_value_employeecity_39 = rail.RepliconServiceOperator(
            task_id='update_text_value_employeecity_39',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2_27').uri }}",
                "customFieldUri": "{{ dag_run.conf.udfuri_employeecity }}",
                "value": "{{ dag_run.conf.employeecity }}"
            }
        )

        if_request_punchtimenetry_present_40 = rail.IfOperator(
            task_id='if_request_punchtimenetry_present_40',
            test='''{{ dag_run.conf.punchtimenetry | is_truthy }}''',
            yes_task="assign_policy_set_to_user_punchentrypolicy_41",
            no_task="if_request_timesheettemplate_present_43",
        )

        assign_policy_set_to_user_punchentrypolicy_41 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_punchentrypolicy_41',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ result('put_user2_27').uri }}",
                "policySetUri": "{{ dag_run.conf.punchtimenetry }}"
            }
        )

        put_place_assignment_schedule_for_user_42 = rail.RepliconServiceOperator(
            task_id='put_place_assignment_schedule_for_user_42',
            endpoint="/services/PlaceService1.svc/PutPlaceAssignmentScheduleForUser",
            data={
                "userTarget": {
                    "uri": "{{ result('put_user2_27').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "scheduleEntries": [
                    {
                        "effectiveDate": null,
                        "places": []
                    }
                ]
            }
        )

        if_request_timesheettemplate_present_43 = rail.IfOperator(
            task_id='if_request_timesheettemplate_present_43',
            test='''{{ dag_run.conf.timesheettemplate | is_truthy }}''',
            yes_task="assign_policy_set_to_usertimesheettemplate_44",
            no_task="if_request_timeofftemplate_present_45",
        )

        assign_policy_set_to_usertimesheettemplate_44 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_usertimesheettemplate_44',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ result('put_user2_27').uri }}",
                "policySetUri": "{{ dag_run.conf.timesheettemplate }}"
            }
        )

        if_request_timeofftemplate_present_45 = rail.IfOperator(
            task_id='if_request_timeofftemplate_present_45',
            test='''{{ dag_run.conf.timeofftemplate | is_truthy }}''',
            yes_task="assign_policy_set_to_usertimeofftemplate_46",
            no_task="if_request_expensetemplate_present_47",
        )

        assign_policy_set_to_usertimeofftemplate_46 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_usertimeofftemplate_46',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ result('put_user2_27').uri }}",
                "policySetUri": "{{ dag_run.conf.timeofftemplate }}"
            }
        )

        if_request_expensetemplate_present_47 = rail.IfOperator(
            task_id='if_request_expensetemplate_present_47',
            test='''{{ dag_run.conf.expensetemplate | is_truthy }}''',
            yes_task="assign_policy_set_to_userexpensetemplate_48",
            no_task="if_request_payrule_present_49",
        )

        assign_policy_set_to_userexpensetemplate_48 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_userexpensetemplate_48',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ result('put_user2_27').uri }}",
                "policySetUri": "{{ dag_run.conf.expensetemplate }}"
            }
        )

        if_request_payrule_present_49 = rail.IfOperator(
            task_id='if_request_payrule_present_49',
            test='''{{ dag_run.conf.payrule | is_truthy }}''',
            yes_task="put_pay_rule_script_assignment_schedule_for_user_50",
            no_task="if_request_license_present_51",
        )

        put_pay_rule_script_assignment_schedule_for_user_50 = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user_50',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_27').uri }}",
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": null,
                            "name": "{{ dag_run.conf.payrule }}"
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_request_license_present_51 = rail.IfOperator(
            task_id='if_request_license_present_51',
            test='''{{ dag_run.conf.license | is_truthy }}''',
            yes_task="put_product_assignments_for_user_52",
            no_task="if_request_primaryroles_present_53",
        )

        put_product_assignments_for_user_52 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_52',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda dag_run:{
                "userUri": rail.result('put_user2_27')['uri'],
                "productUris": dag_run.conf['license']
            }
        )

        if_request_primaryroles_present_53 = rail.IfOperator(
            task_id='if_request_primaryroles_present_53',
            test='''{{ dag_run.conf.primaryroles | is_truthy  or dag_run.conf.addtionalroles | is_truthy }}''',
            yes_task="invoke_custom_py_code_54",
            no_task="if_request_permissions_present_56",
        )

        def get_project_roles(dag_run):
            primaryroles = [dag_run.conf['primaryroles']
                            ] if dag_run.conf['primaryroles'] else []
            additionalroles = list(set((rail.smartjoin_by_delim((dag_run.conf['additionalroles']).split(
                "|"), "|")).split("|"))) if dag_run.conf['additionalroles'] else []
            merge_list = primaryroles + additionalroles
            final_list = list(map(lambda item: {"name": item}, merge_list))
            return {"rolestoadd": final_list}

        invoke_custom_py_code_54 = rail.PythonOperator(
            task_id='invoke_custom_py_code_54',
            python_callable=get_project_roles
        )

        adhoc_http_action_55 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_55',
            endpoint="/services/ResourceService1.svc/PutProjectRoleAssignmentScheduleForUser",
            data=lambda dag_run: {
                "scheduleEntries": [
                    {
                        "projectRoles": [{
                            "projectRole": {
                                "name": roletoadd['name']
                            },
                            "isPrimary": "true" if roletoadd['name'] == dag_run.conf['primaryroles'] else "false"
                        } for roletoadd in rail.result('invoke_custom_py_code_54')['rolestoadd']]
                    }
                ],
                "userUri": rail.result('put_user2_27')['uri']
            }
        )

        if_request_permissions_present_56 = rail.IfOperator(
            task_id='if_request_permissions_present_56',
            test='''{{ dag_run.conf.permissions | is_truthy }}''',
            yes_task="put_permission_set_assignments_for_user_57",
            no_task="if_request_supervisorid_present_58",
        )

        put_permission_set_assignments_for_user_57 = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_user_57',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run:{
                "userUri": rail.result('put_user2_27')['uri'],
                "permissionSetUris": dag_run.conf['permissions']
            }
        )

        if_request_supervisorid_present_58 = rail.IfOperator(
            task_id='if_request_supervisorid_present_58',
            test='''{{ dag_run.conf.supervisorid | is_truthy }}''',
            yes_task="search_users_59",
            no_task="if_declare_list_3_list_items_greater_than_0_76",
        )

        def get_supervisoruser_details(response, dag_run):
            users_found = response['rows']
            required_user = {}
            for user in users_found:
                if user['cells'][1]['textValue'] == dag_run.conf['supervisorid']:
                    required_user = user
            return {
                'user': required_user if users_found and required_user else '',
                'useruri': required_user['cells'][0]['uri'] if users_found and required_user else '',
                'enabled': required_user['cells'][2]['boolValue'] if users_found and required_user else False,
            }

        search_users_59 = rail.RepliconServiceOperator(
            task_id='search_users_59',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{dag_run.conf.supervisorid}}"
                        }
                    }
                }
            },
            data_handler=get_supervisoruser_details
        )

        if_log_checkifuserexist_60_present_61 = rail.IfOperator(
            task_id='if_log_checkifuserexist_60_present_61',
            test='''{{ result('search_users_59').useruri | is_truthy }}''',
            yes_task="if_enabled_boolvalue_is_true_62",
            no_task="npsg_supervisor_check_add_entry_75",
        )

        if_enabled_boolvalue_is_true_62 = rail.IfOperator(
            task_id='if_enabled_boolvalue_is_true_62',
            test='''{{ result('search_users_59').enabled | is_truthy }}''',
            yes_task="get_assigned_permission_sets_for_user2_63",
            no_task="insert_to_list_73",
        )

        get_assigned_permission_sets_for_user2_63 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_63',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_59').useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'permissionSet.name', 'Supervisor', 'permissionSet')
        )

        if_log_checkifsupervisorpermissionsetisassigned_64_present_65 = rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionsetisassigned_64_present_65',
            test='''{{ result('get_assigned_permission_sets_for_user2_63') | is_truthy }}''',
            yes_task="put_supervisor_assignment_schedule_66",
            no_task="if_log_checkifsupervisorpermissionsetisassigned_64_blank_67",
        )

        put_supervisor_assignment_schedule_66 = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule_66',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ result('put_user2_27').uri }}",
                "initialSupervisorUri": "{{ result('search_users_59').useruri }}",
                "scheduleEntries": []
            }
        )

        if_log_checkifsupervisorpermissionsetisassigned_64_blank_67 = rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionsetisassigned_64_blank_67',
            test='''{{ result('get_assigned_permission_sets_for_user2_63') | is_falsy }}''',
            yes_task="get_all_permission_sets_68",
            no_task="if_declare_list_3_list_items_greater_than_0_76",
        )

        get_all_permission_sets_68 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_68',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', 'Supervisor', 'uri', '')
        )

        assign_permission_set_to_user_70 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_70',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_59').useruri }}",
                "permissionSetUri": "{{ result('get_all_permission_sets_68') }}"
            }
        )

        put_supervisor_assignment_schedule_71 = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule_71',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ result('put_user2_27').uri }}",
                "initialSupervisorUri": "{{ result('search_users_59').useruri }}",
                "scheduleEntries": []
            }
        )

        insert_to_list_73 = rail.SetVariableOperator(
            task_id='insert_to_list_73',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Supervisor not assignes as the required Supervisor is in disabled status."
            }
        )

        npsg_supervisor_check_add_entry_75 = rail.WriteLogOperator(
            task_id='npsg_supervisor_check_add_entry_75',
            log="{{ dag_run.conf.supervisorlookup }}",
            message="na",
            severity="na",
            properties=lambda: {
                "jobid": "{{dag_run.conf.callerjobid}}",
                "userempid": "{{ dag_run.conf.employeeid }}",
                "useruri": "{{ result('put_user2_27').uri }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorempid": "{{ dag_run.conf.supervisorid }}",
                "action": "add",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": '',
                "effectivedate": datetime.now().strftime("%m/%d/%Y")
            }
        )

        if_declare_list_3_list_items_greater_than_0_76 = rail.IfOperator(
            task_id='if_declare_list_3_list_items_greater_than_0_76',
            test=lambda: len(rail.get_dag_run_var('exception')) > 0,
            yes_task="npsg_user_import_logs_add_entry_77",
            no_task="npsg_user_import_logs_add_entry_82",
        )

        npsg_user_import_logs_add_entry_77 = rail.WriteLogOperator(
            task_id='npsg_user_import_logs_add_entry_77',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="exception",
            properties=lambda dag_run: {
                "empid": dag_run.conf['employeeid'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "action": "add",
                "status": "exception",
                "details": "User created successfully. " + rail.get_dag_run_var('exception')[0]['log'],
                "parentjob": dag_run.conf['callerjobid'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        npsg_user_import_logs_add_entry_82 = rail.WriteLogOperator(
            task_id='npsg_user_import_logs_add_entry_82',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="success",
            properties={
                "empid": "{{dag_run.conf.employeeid}}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "add",
                "status": "success",
                "details": "User created successfully. ",
                "parentjob": "{{dag_run.conf.callerjobid}}",
                "childjob": "{{ dag_run_ecid() }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.userimportlogtable}}",
            message="na",
            severity="error",
            properties={
                "empid": "{{dag_run.conf.employeeid}}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "add",
                "status": "error",
                "details": "{{get_error_message()}}",
                "parentjob": "{{dag_run.conf.callerjobid}}",
                "childjob": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> declare_list_3
        declare_list_3 >> check_for_exceptions >> if_log_exceptionlog_8_present_9
        if_log_exceptionlog_8_present_9 >> rail.Label(
            'Yes') >> insert_to_list_10 >> search_users_11
        if_log_exceptionlog_8_present_9 >> rail.Label(
            'No') >> search_users_11 >> if_output_user_present_13
        if_output_user_present_13 >> rail.Label(
            'Yes') >> if_output_enabled_is_not_true_14
        if_output_enabled_is_not_true_14 >> rail.Label(
            'Yes') >> trigger_dag_run_live_npsgeu_update_user_v1_015 >> wait_for_completion_trigger_dag_run_live_npsgeu_update_user_v1_015
        wait_for_completion_trigger_dag_run_live_npsgeu_update_user_v1_015 >> catch_and_log_error
        if_output_enabled_is_not_true_14 >> rail.Label(
            'No') >> search_user_by_loginname >> if_log_checkifuserexistwithsameloginname_19_present_20
        if_output_user_present_13 >> rail.Label(
            'No') >> search_user_by_loginname >> if_log_checkifuserexistwithsameloginname_19_present_20
        if_log_checkifuserexistwithsameloginname_19_present_20 >> rail.Label(
            'Yes') >> npsg_user_import_logs_add_entry_21 >> catch_and_log_error
        if_log_checkifuserexistwithsameloginname_19_present_20 >> rail.Label(
            'No') >> if_request_enddate_present_23
        if_request_enddate_present_23 >> rail.Label(
            'Yes') >> npsg_user_import_logs_add_entry_24 >> catch_and_log_error
        if_request_enddate_present_23 >> rail.Label(
            'No') >> put_user2_27 >> if_request_locationuri_present_28
        if_request_locationuri_present_28 >> rail.Label(
            'Yes') >> apply_user_modifications2location_29 >> if_request_employmentstatus_present_30
        if_request_locationuri_present_28 >> rail.Label(
            'No') >> if_request_employmentstatus_present_30
        if_request_employmentstatus_present_30 >> rail.Label(
            'Yes') >> update_dropdown_value_employementstatus_31 >> if_request_division_present_32
        if_request_employmentstatus_present_30 >> rail.Label(
            'No') >> if_request_division_present_32
        if_request_division_present_32 >> rail.Label(
            'Yes') >> update_dropdown_value_division_33 >> if_request_position_present_34
        if_request_division_present_32 >> rail.Label(
            'No') >> if_request_position_present_34
        if_request_position_present_34 >> rail.Label(
            'Yes') >> update_dropdown_value_position_35 >> if_request_employeestate_present_36
        if_request_position_present_34 >> rail.Label(
            'No') >> if_request_employeestate_present_36
        if_request_employeestate_present_36 >> rail.Label(
            'Yes') >> update_text_value_employeestate_37 >> if_request_employeecity_present_38
        if_request_employeestate_present_36 >> rail.Label(
            'No') >> if_request_employeecity_present_38
        if_request_employeecity_present_38 >> rail.Label(
            'Yes') >> update_text_value_employeecity_39 >> if_request_punchtimenetry_present_40
        if_request_employeecity_present_38 >> rail.Label(
            'No') >> if_request_punchtimenetry_present_40
        if_request_punchtimenetry_present_40 >> rail.Label(
            'Yes') >> assign_policy_set_to_user_punchentrypolicy_41 >> put_place_assignment_schedule_for_user_42 >> if_request_timesheettemplate_present_43
        if_request_punchtimenetry_present_40 >> rail.Label(
            'No') >> if_request_timesheettemplate_present_43
        if_request_timesheettemplate_present_43 >> rail.Label(
            'Yes') >> assign_policy_set_to_usertimesheettemplate_44 >> if_request_timeofftemplate_present_45
        if_request_timesheettemplate_present_43 >> rail.Label(
            'No') >> if_request_timeofftemplate_present_45
        if_request_timeofftemplate_present_45 >> rail.Label(
            'Yes') >> assign_policy_set_to_usertimeofftemplate_46 >> if_request_expensetemplate_present_47
        if_request_timeofftemplate_present_45 >> rail.Label(
            'No') >> if_request_expensetemplate_present_47
        if_request_expensetemplate_present_47 >> rail.Label(
            'Yes') >> assign_policy_set_to_userexpensetemplate_48 >> if_request_payrule_present_49
        if_request_expensetemplate_present_47 >> rail.Label(
            'No') >> if_request_payrule_present_49
        if_request_payrule_present_49 >> rail.Label(
            'Yes') >> put_pay_rule_script_assignment_schedule_for_user_50 >> if_request_license_present_51
        if_request_payrule_present_49 >> rail.Label(
            'No') >> if_request_license_present_51
        if_request_license_present_51 >> rail.Label(
            'Yes') >> put_product_assignments_for_user_52 >> if_request_primaryroles_present_53
        if_request_license_present_51 >> rail.Label(
            'No') >> if_request_primaryroles_present_53
        if_request_primaryroles_present_53 >> rail.Label(
            'Yes') >> invoke_custom_py_code_54 >> adhoc_http_action_55 >> if_request_permissions_present_56
        if_request_primaryroles_present_53 >> rail.Label(
            'No') >> if_request_permissions_present_56
        if_request_permissions_present_56 >> rail.Label(
            'Yes') >> put_permission_set_assignments_for_user_57 >> if_request_supervisorid_present_58
        if_request_permissions_present_56 >> rail.Label(
            'No') >> if_request_supervisorid_present_58
        if_request_supervisorid_present_58 >> rail.Label(
            'Yes') >> search_users_59 >> if_log_checkifuserexist_60_present_61
        if_log_checkifuserexist_60_present_61 >> rail.Label(
            'Yes') >> if_enabled_boolvalue_is_true_62
        if_enabled_boolvalue_is_true_62 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_63 >> if_log_checkifsupervisorpermissionsetisassigned_64_present_65
        if_log_checkifsupervisorpermissionsetisassigned_64_present_65 >> rail.Label(
            'Yes') >> put_supervisor_assignment_schedule_66 >> if_log_checkifsupervisorpermissionsetisassigned_64_blank_67
        if_log_checkifsupervisorpermissionsetisassigned_64_present_65 >> rail.Label(
            'No') >> if_log_checkifsupervisorpermissionsetisassigned_64_blank_67
        if_log_checkifsupervisorpermissionsetisassigned_64_blank_67 >> rail.Label(
            'Yes') >> get_all_permission_sets_68 >> assign_permission_set_to_user_70 >> put_supervisor_assignment_schedule_71
        put_supervisor_assignment_schedule_71 >> if_declare_list_3_list_items_greater_than_0_76
        if_log_checkifsupervisorpermissionsetisassigned_64_blank_67 >> rail.Label(
            'No') >> if_declare_list_3_list_items_greater_than_0_76
        if_enabled_boolvalue_is_true_62 >> rail.Label(
            'No') >> insert_to_list_73 >> if_declare_list_3_list_items_greater_than_0_76
        if_log_checkifuserexist_60_present_61 >> rail.Label(
            'No') >> npsg_supervisor_check_add_entry_75 >> if_declare_list_3_list_items_greater_than_0_76
        if_request_supervisorid_present_58 >> rail.Label(
            'No') >> if_declare_list_3_list_items_greater_than_0_76
        if_declare_list_3_list_items_greater_than_0_76 >> rail.Label(
            'Yes') >> npsg_user_import_logs_add_entry_77 >> catch_and_log_error
        if_declare_list_3_list_items_greater_than_0_76 >> rail.Label(
            'No') >> npsg_user_import_logs_add_entry_82 >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)

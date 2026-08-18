from datetime import timedelta, datetime
from pendulum import now
import json
from airflow.models import Variable
import rail
from fujifilmdbtl.user_import.utils import request_payload, python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdbtl_child_add_user_{config.instance}',
        description=f'FDT_Child_Workflow to add user {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config
        )
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_request_servicedate_blank_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_servicedate_blank_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_servicedate_blank_3 = rail.IfOperator(
            task_id='if_request_servicedate_blank_3',
            test=lambda dag_run: not (
                dag_run.conf['servicedate']) or "/" not in (dag_run.conf['servicedate']),
            yes_task="fdt_user_import_logs_add_entry_4",
            no_task="declare_variable_6",
        )

        fdt_user_import_logs_add_entry_4 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entry_4',
            log=lambda dag_run: dag_run.conf['userimportlogtable'],
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "username": rail.render_template("{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"),
                "loginname": dag_run.conf['loginname'],
                "emplid": dag_run.conf['emplid'],
                "action": "Add",
                "status": "Exception",
                "details": ("Incorrect Log" if "/" in dag_run.conf['servicedate'] else "Service Date is not in the correct format") if dag_run.conf['servicedate'] else "Service Date is not present in the feed file",
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        declare_variable_6 = rail.SetVariableOperator(
            task_id='declare_variable_6',
            append=False,
            name='employeetype',
            value=""
        )

        if_request_eetype_blank_7 = rail.IfOperator(
            task_id='if_request_eetype_blank_7',
            test='''{{ dag_run.conf.eetype | is_falsy }}''',
            yes_task="fdt_user_import_logs_add_entry_8",
            no_task="invoke_custom_ruby_code_service_date_10",
        )

        fdt_user_import_logs_add_entry_8 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entry_8',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Exception",
            properties={
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "emplid": "{{ dag_run.conf.emplid }}",
                "action": "Add",
                "status": "Exception",
                "details": "EeType is not present in feed file",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        invoke_custom_ruby_code_service_date_10 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_service_date_10',
            python_callable=lambda dag_run: python_callable.get_split_date(
                dag_run.conf['servicedate'], "%m/%d/%Y")
        )

        invoke_custom_ruby_code_todays_date_11 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_todays_date_11',
            python_callable=lambda: python_callable.get_split_date(
                now())
        )

        if_request_eetype_equals_to_s_12 = rail.IfOperator(
            task_id='if_request_eetype_equals_to_s_12',
            test='''{{ dag_run.conf.eetype == 's' }}''',
            yes_task="update_variable_13",
            no_task="if_request_eetype_equals_to_h_14",
        )

        update_variable_13 = rail.SetVariableOperator(
            task_id='update_variable_13',
            append=False,
            name='employeetype',
            value="Salaried"
        )

        if_request_eetype_equals_to_h_14 = rail.IfOperator(
            task_id='if_request_eetype_equals_to_h_14',
            test='''{{ dag_run.conf.eetype == 'h' }}''',
            yes_task="update_variable_15",
            no_task="_adhoc_http_action_16",
        )

        update_variable_15 = rail.SetVariableOperator(
            task_id='update_variable_15',
            append=False,
            name='employeetype',
            value="Hourly"
        )

        _adhoc_http_action_16 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_16',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
        )

        log_employee_type_uri_17 = rail.PythonOperator(
            task_id='log_employee_type_uri_17',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('_adhoc_http_action_16'), 'name', rail.get_dag_run_var(
                'employeetype'), 'uri', "") if (rail.result('_adhoc_http_action_16') and rail.result('_adhoc_http_action_16')[0]['name']) else ""
        )

        if_log_employee_type_uri_17_blank_18 = rail.IfOperator(
            task_id='if_log_employee_type_uri_17_blank_18',
            test='''{{ result('log_employee_type_uri_17') | is_falsy }}''',
            yes_task="fdt_user_import_logs_add_entry_19",
            no_task="_adhoc_http_action_21",
        )

        fdt_user_import_logs_add_entry_19 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entry_19',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Exception",
            properties={
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "emplid": "{{ dag_run.conf.emplid }}",
                "action": "Add",
                "status": "Exception",
                "details": "Eetype {{ dag_run.conf.eetype }} is not present in Replicon",
                "childjobid": "{{dag_run_ecid()}}"
            }
        )

        _adhoc_http_action_21 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_21',
            endpoint="/services/DepartmentService1.svc/GetCompanyDepartment",
        )

        if_request_deptid_present_22 = rail.IfOperator(
            task_id='if_request_deptid_present_22',
            test='''{{ dag_run.conf.deptid | is_truthy }}''',
            yes_task="get_datafordepartmentbasedonnameandcode_23",
            no_task="log_error_logfordepartmentcodenotpresent_27",
        )

        get_datafordepartmentbasedonnameandcode_23 = rail.RepliconServiceOperator(
            task_id='get_datafordepartmentbasedonnameandcode_23',
            endpoint="/services/DepartmentListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_data_for_department_based_on_name_and_code(
                dag_run.conf['department'], dag_run.conf['deptid']),
            data_handler=lambda response: response['rows']
        )

        invoke_custom_ruby_code_24 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_24',
            python_callable=lambda: python_callable.get_department_list_output(
                rail.result('get_datafordepartmentbasedonnameandcode_23'))
        )

        log_departmenturi_25 = rail.PythonOperator(
            task_id='log_departmenturi_25',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'invoke_custom_ruby_code_24'), 'name', dag_run.conf['department'], 'uri', "") if rail.result('invoke_custom_ruby_code_24') else ""
        )

        log_error_logfordepartmentcodenotpresent_27 = rail.PythonOperator(
            task_id='log_error_logfordepartmentcodenotpresent_27',
            python_callable=lambda:  rail.render_template(
                "Department not added for User {{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}. DeptID is not available in the feed file.  Hence, {{result('_adhoc_http_action_21').name }} is added as department for the user")
        )

        log_requireddepartment_28 = rail.PythonOperator(
            task_id='log_requireddepartment_28',
            python_callable=lambda:  rail.result('log_departmenturi_25') if rail.result(
                'log_departmenturi_25') else rail.result('_adhoc_http_action_21')['uri']
        )

        _adhoc_http_action_29 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_29',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        log_biweekly_wages_30 = rail.PythonOperator(
            task_id='log_biweekly_wages_30',
            python_callable=lambda dag_run: ((round((float(dag_run.conf['annualsalary']) / 26), 2) if dag_run.conf['annualsalary'] else 0) if (
                "f" in dag_run.conf['fullparttime'].lower()) else 0) if dag_run.conf['fullparttime'] else 0
        )

        declare_variable_31 = rail.SetVariableOperator(
            task_id='declare_variable_31',
            append=False,
            name='schedule',
            value="8 Hours/Day; Mon-Fri"
        )

        if_request_fullparttime_not_equals_to_f_32 = rail.IfOperator(
            task_id='if_request_fullparttime_not_equals_to_f_32',
            test='''{{ dag_run.conf.fullparttime != 'f' }}''',
            yes_task="update_variable_33",
            no_task="create_user_34",
        )

        update_variable_33 = rail.SetVariableOperator(
            task_id='update_variable_33',
            append=False,
            name='schedule',
            value="4 Hours/Day; Mon-Fri"
        )

        create_user_34 = rail.RepliconServiceOperator(
            task_id='create_user_34',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['loginname'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": dag_run.conf['email'],
                    "employeeId": dag_run.conf['emplid'],
                    "department": {
                        "uri": rail.result('log_requireddepartment_28'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": rail.get_dag_run_var('schedule'),
                                "officeSchedule": null,
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "workWeekStartDayUri": "urn:replicon:day-of-week:sunday",
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('invoke_custom_ruby_code_service_date_10')['year'],
                            "month": rail.result('invoke_custom_ruby_code_service_date_10')['month'],
                            "day": rail.result('invoke_custom_ruby_code_service_date_10')['day']
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
                        "loginName": dag_run.conf['loginname'],
                        "SSOName": dag_run.conf['loginname'],
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
                            "name": "Standard Timesheet"
                        },
                        {
                            "uri": null,
                            "name": "Time Off"
                        }
                    ],
                    "employeeType": {
                        "uri": rail.result('log_employee_type_uri_17'),
                        "name": null
                    },
                    "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:system",
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {
                        "uri": null,
                        "name": "Supervisor"
                    },
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": {
                        "uri": null,
                        "name": "Supervisor"
                    },
                    "customFieldValues": [
                        {
                            "customField": {
                                "uri": null,
                                "name": "Regular/Temporary",
                                "groupUri": rail.result('_adhoc_http_action_29')[0]['group']['uri']
                            },
                            "text": dag_run.conf['regulartemporary'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": null,
                                "name": "Job Title",
                                "groupUri": rail.result('_adhoc_http_action_29')[0]['group']['uri']
                            },
                            "text": dag_run.conf['jobtitle'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": null,
                                "name": "Full/Part Time",
                                "groupUri": rail.result('_adhoc_http_action_29')[0]['group']['uri']
                            },
                            "text": dag_run.conf['fullparttime'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": null,
                                "name": "Company",
                                "groupUri": rail.result('_adhoc_http_action_29')[0]['group']['uri']
                            },
                            "text": dag_run.conf['company'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": null,
                                "name": "EE Status",
                                "groupUri": rail.result('_adhoc_http_action_29')[0]['group']['uri']
                            },
                            "text": dag_run.conf['eestatus'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": null,
                                "name": "Pay Group",
                                "groupUri": rail.result('_adhoc_http_action_29')[0]['group']['uri']
                            },
                            "text": dag_run.conf['paygroup'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": null,
                                "name": "Manager ID",
                                "groupUri": rail.result('_adhoc_http_action_29')[0]['group']['uri']
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": null,
                            "number": dag_run.conf['managerid']
                        },
                        {
                            "customField": {
                                "uri": null,
                                "name": "File Number",
                                "groupUri": rail.result('_adhoc_http_action_29')[0]['group']['uri']
                            },
                            "text": dag_run.conf['file'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": null,
                                "name": "Autolink Rate Type",
                                "groupUri": rail.result('_adhoc_http_action_29')[0]['group']['uri']
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": null,
                            "number": dag_run.conf['autolinkratetype']
                        },
                        {
                            "customField": {
                                "uri": null,
                                "name": "Gross Annual Salary",
                                "groupUri": rail.result('_adhoc_http_action_29')[0]['group']['uri']
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": null,
                            "number": dag_run.conf['annualsalary']
                        },
                        {
                            "customField": {
                                "uri": null,
                                "name": "Assigned Shift",
                                "groupUri": rail.result('_adhoc_http_action_29')[0]['group']['uri']
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": null,
                                "name": dag_run.conf['assignedshift']
                            },
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": null,
                                "name": "Biweekly Gross Wages",
                                "groupUri": rail.result('_adhoc_http_action_29')[0]['group']['uri']
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": null,
                            "number": rail.result('log_biweekly_wages_30')
                        }
                    ],
                    "assignedActivities": [],
                    "timeZone": {
                        "uri": "urn:replicon:time-zone:america-chicago",
                        "IANAName": null
                    },
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        remove_timeoffassignmentsforusers_35 = rail.RepliconServiceOperator(
            task_id='remove_timeoffassignmentsforusers_35',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_34').uri }}",
                "timeOffTypeUris": []
            }
        )

        if_request_regulartemporary_equals_to_t_36 = rail.IfOperator(
            task_id='if_request_regulartemporary_equals_to_t_36',
            test='''{{ dag_run.conf.regulartemporary == 't' }}''',
            yes_task="remove_holiday_calendarassignmentsforusers_37",
            no_task="log_service_date_u_d_furi_38",
        )

        remove_holiday_calendarassignmentsforusers_37 = rail.RepliconServiceOperator(
            task_id='remove_holiday_calendarassignmentsforusers_37',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_user_34').uri }}",
                "holidayCalendarUri": null
            }
        )

        log_service_date_u_d_furi_38 = rail.PythonOperator(
            task_id='log_service_date_u_d_furi_38',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_29'), 'displayText', "Service Date", 'uri', "") if rail.result('_adhoc_http_action_29')[0]['displayText'] else ""
        )

        if_log_service_date_u_d_furi_38_present_39 = rail.IfOperator(
            task_id='if_log_service_date_u_d_furi_38_present_39',
            test='''{{ result('log_service_date_u_d_furi_38') | is_truthy }}''',
            yes_task="update_service_date_u_d_f_40",
            no_task="log_adjusted_service_date_u_d_furi_41",
        )

        update_service_date_u_d_f_40 = rail.RepliconServiceOperator(
            task_id='update_service_date_u_d_f_40',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ result('log_service_date_u_d_furi_38') }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_service_date_10').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_service_date_10').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_service_date_10').day }}"
                }
            }
        )

        log_adjusted_service_date_u_d_furi_41 = rail.PythonOperator(
            task_id='log_adjusted_service_date_u_d_furi_41',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                "_adhoc_http_action_29"), 'displayText', "Adjusted Service Date", 'uri', "") if rail.result("_adhoc_http_action_29")[0]['displayText'] else ""
        )

        if_log_adjusted_service_date_u_d_furi_41_present_42 = rail.IfOperator(
            task_id='if_log_adjusted_service_date_u_d_furi_41_present_42',
            test='''{{ result('log_adjusted_service_date_u_d_furi_41') | is_truthy }}''',
            yes_task="update_adjusted_service_date_u_d_f_43",
            no_task="log_getrequiredpayrule_44",
        )

        update_adjusted_service_date_u_d_f_43 = rail.RepliconServiceOperator(
            task_id='update_adjusted_service_date_u_d_f_43',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ result('log_adjusted_service_date_u_d_furi_41') }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_service_date_10').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_service_date_10').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_service_date_10').day }}"
                }
            }
        )

        log_getrequiredpayrule_44 = rail.PythonOperator(
            task_id='log_getrequiredpayrule_44',
            python_callable=lambda dag_run: ("FDBT Weekly FT Overtime" if ("f" in dag_run.conf['fullparttime']) else "FDBT Weekly PT Overtime") if (
                "h" in dag_run.conf['eetype'].lower()) else "Salaried-Dummy"
        )

        _adhoc_http_action_45 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_45',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        log_get_pay_rule_script_uri_46 = rail.PythonOperator(
            task_id='log_get_pay_rule_script_uri_46',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('_adhoc_http_action_45'), 'displayText', rail.result(
                'log_getrequiredpayrule_44'), 'uri', "") if rail.result('_adhoc_http_action_45')[0]['displayText'] else ""
        )

        if_log_get_pay_rule_script_uri_46_present_enabled_47 = rail.IfOperator(
            task_id='if_log_get_pay_rule_script_uri_46_present_enabled_47',
            test='''{{ result('log_get_pay_rule_script_uri_46') | is_truthy }}''',
            yes_task="put_payroll_assignment_48",
            no_task="if_request_managerid_present_49",
        )

        put_payroll_assignment_48 = rail.RepliconServiceOperator(
            task_id='put_payroll_assignment_48',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ result('create_user_34').uri }}",
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": "{{ result('log_get_pay_rule_script_uri_46') }}",
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_request_managerid_present_49 = rail.IfOperator(
            task_id='if_request_managerid_present_49',
            test='''{{ dag_run.conf.managerid | is_truthy }}''',
            yes_task="search_users_50",
            no_task="if_request_rehiredate_present_74",
        )

        search_users_50 = rail.RepliconServiceOperator(
            task_id='search_users_50',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_search_user_payload_for_supervisor(
                dag_run.conf['managerid']),
            data_handler=lambda response: response['rows']
        )

        user_search_result_list = rail.PythonOperator(
            task_id='user_search_result_list',
            python_callable=lambda: python_callable.get_search_user_details(
                rail.result('search_users_50'))
        )

        log_getthenumberofuserswithsameemployeeid_51 = rail.PythonOperator(
            task_id='log_getthenumberofuserswithsameemployeeid_51',
            python_callable=lambda: len(rail.result('search_users_50')) if rail.result(
                'search_users_50') else ""
        )

        if_log_getthenumberofuserswithsameemployeeid_51_present_1_52 = rail.IfOperator(
            task_id='if_log_getthenumberofuserswithsameemployeeid_51_present_1_52',
            test='''{{ result('log_getthenumberofuserswithsameemployeeid_51') | is_truthy and result('log_getthenumberofuserswithsameemployeeid_51') > 1 }}''',
            yes_task="log_errorforsupervisorassignment_53",
            no_task="log_getsupervisor_uri_55",
        )

        log_errorforsupervisorassignment_53 = rail.PythonOperator(
            task_id='log_errorforsupervisorassignment_53',
            python_callable=lambda dag_run:  "Supervisor assignment skipped as multiple users have same employee id as " +
            dag_run.conf['managerid']
        )

        log_getsupervisor_uri_55 = rail.PythonOperator(
            task_id='log_getsupervisor_uri_55',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'user_search_result_list'), 'employeeid', dag_run.conf['managerid'], 'uri', "") if rail.result('user_search_result_list') else null
        )

        log_getsupervisorloginname_56 = rail.PythonOperator(
            task_id='log_getsupervisorloginname_56',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'user_search_result_list'), 'employeeid', dag_run.conf['managerid'], 'loginname', "") if rail.result('user_search_result_list') else null
        )

        _adhoc_http_action_57 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_57',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        if_request_loginname_not_equals_to_dataloggerlog_getsupervisorloginname_56message_58 = rail.IfOperator(
            task_id='if_request_loginname_not_equals_to_dataloggerlog_getsupervisorloginname_56message_58',
            test='''{{ dag_run.conf.loginname != result('log_getsupervisorloginname_56') }}''',
            yes_task="if_log_getsupervisor_uri_55_present_59",
            no_task="log_errorwhenuserandsupervisorsloginnamearesame_73",
        )

        if_log_getsupervisor_uri_55_present_59 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_55_present_59',
            test='''{{ result('log_getsupervisor_uri_55') | is_truthy }}''',
            yes_task="log_get_supervisor_status_60",
            no_task="if_log_getsupervisor_uri_55_blank_70",
        )

        log_get_supervisor_status_60 = rail.PythonOperator(
            task_id='log_get_supervisor_status_60',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'user_search_result_list'), 'employeeid', dag_run.conf['managerid'], 'enabled', "") if rail.result('user_search_result_list') else null
        )

        if_log_get_supervisor_status_60_equals_to_true_61 = rail.IfOperator(
            task_id='if_log_get_supervisor_status_60_equals_to_true_61',
            test='''{{ result('log_get_supervisor_status_60') == "True" }}''',
            yes_task="_adhoc_http_action_62",
            no_task="fdt_supervisor_assignment_table_add_entry_69",
        )

        _adhoc_http_action_62 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_62',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('log_getsupervisor_uri_55') }}"
            }
        )

        log_checkifsupervisorhassupervisorpermission_63 = rail.PythonOperator(
            task_id='log_checkifsupervisorhassupervisorpermission_63',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_62'), 'policyUri', "urn:replicon:policy:supervision", 'permissionSet', "") if rail.result('_adhoc_http_action_62')[0]['policyUri'] else ""
        )

        if_log_checkifsupervisorhassupervisorpermission_63_blank_64 = rail.IfOperator(
            task_id='if_log_checkifsupervisorhassupervisorpermission_63_blank_64',
            test='''{{ result('log_checkifsupervisorhassupervisorpermission_63') | is_falsy }}''',
            yes_task="log_get_supervisor_permission_65",
            no_task="update_initial_supervisor_67",
        )

        log_get_supervisor_permission_65 = rail.PythonOperator(
            task_id='log_get_supervisor_permission_65',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('_adhoc_http_action_57'), 'displayText', "Supervisor", 'uri', "")
        )

        assign_supervsior_permission_set_to_user_66 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_66',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('log_getsupervisor_uri_55') }}",
                "permissionSetUri": "{{ result('log_get_supervisor_permission_65') }}"
            }
        )

        update_initial_supervisor_67 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_67',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ result('create_user_34').uri }}",
                "initialSupervisorUri": "{{ result('log_getsupervisor_uri_55') }}",
                "scheduleEntries": []
            }
        )

        fdt_supervisor_assignment_table_add_entry_69 = rail.WriteLogOperator(
            task_id='fdt_supervisor_assignment_table_add_entry_69',
            log="{{dag_run.conf.supervisorassignmentlookuptable}}",
            message="na",
            severity="pending",
            properties={
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "userloginname": "{{ dag_run.conf.loginname }}",
                "user_uri": "{{ result('create_user_34').uri }}",
                "user_name": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorloginname": "{{ result('log_getsupervisorloginname_56') }}",
                "supervisor_id": "{{ dag_run.conf.managerid }}",
                "action": "Add",
                "emplid": "{{ dag_run.conf.emplid }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": "pending"
            }
        )

        if_log_getsupervisor_uri_55_blank_70 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_55_blank_70',
            test='''{{ result('log_getsupervisor_uri_55') | is_falsy }}''',
            yes_task="fdt_supervisor_assignment_table_add_entry_71",
            no_task="if_request_rehiredate_present_74",
        )

        fdt_supervisor_assignment_table_add_entry_71 = rail.WriteLogOperator(
            task_id='fdt_supervisor_assignment_table_add_entry_71',
            log="{{dag_run.conf.supervisorassignmentlookuptable}}",
            message="na",
            severity="pending",
            properties={
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "userloginname": "{{ dag_run.conf.loginname }}",
                "user_uri": "{{ result('create_user_34').uri }}",
                "user_name": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorloginname": "{{ result('log_getsupervisorloginname_56') }}",
                "supervisor_id": "{{ dag_run.conf.managerid }}",
                "action": "Add",
                "emplid": "{{ dag_run.conf.emplid }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": "pending"
            }
        )

        log_errorwhenuserandsupervisorsloginnamearesame_73 = rail.PythonOperator(
            task_id='log_errorwhenuserandsupervisorsloginnamearesame_73',
            python_callable=lambda: rail.render_template(
                "User {{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} is created, however supervisor is not updated as the Login name for user and supervisor is same;")
        )

        if_request_rehiredate_present_74 = rail.IfOperator(
            task_id='if_request_rehiredate_present_74',
            test='''{{ dag_run.conf.rehiredate | is_truthy }}''',
            yes_task="if_request_rehiredate_not_contains_75",
            no_task="get_base_currency_details_83",
        )

        if_request_rehiredate_not_contains_75 = rail.IfOperator(
            task_id='if_request_rehiredate_not_contains_75',
            test=lambda dag_run: "/" not in dag_run.conf['rehiredate'],
            yes_task="log_logforincorrectrehiredateformat_76",
            no_task="invoke_custom_ruby_code_rehire_date_79",
        )

        log_logforincorrectrehiredateformat_76 = rail.PythonOperator(
            task_id='log_logforincorrectrehiredateformat_76',
            python_callable=lambda: "Rehire date is not in the correct format"
        )

        invoke_custom_ruby_code_rehire_date_79 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_rehire_date_79',
            python_callable=lambda dag_run: python_callable.get_split_date(
                dag_run.conf['rehiredate'], "%m/%d/%Y")
        )

        log_rehire_date_u_d_furi_80 = rail.PythonOperator(
            task_id='log_rehire_date_u_d_furi_80',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_29'), 'displayText', "Rehire Start Date", 'uri', "") if rail.result('_adhoc_http_action_29')[0]['displayText'] else ""
        )

        if_log_rehire_date_u_d_furi_80_present_81 = rail.IfOperator(
            task_id='if_log_rehire_date_u_d_furi_80_present_81',
            test='''{{ result('log_rehire_date_u_d_furi_80') | is_truthy }}''',
            yes_task="update_rehire_date_u_d_f_82",
            no_task="get_base_currency_details_83",
        )

        update_rehire_date_u_d_f_82 = rail.RepliconServiceOperator(
            task_id='update_rehire_date_u_d_f_82',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ result('log_rehire_date_u_d_furi_80') }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_rehire_date_79').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_rehire_date_79').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_rehire_date_79').day }}"
                }
            }
        )

        get_base_currency_details_83 = rail.RepliconServiceOperator(
            task_id='get_base_currency_details_83',
            endpoint="/services/CurrencyService2.svc/GetBaseCurrency",
        )

        if_request_hourlyratejobdata_present_84 = rail.IfOperator(
            task_id='if_request_hourlyratejobdata_present_84',
            test='''{{ dag_run.conf.hourlyratejobdata | is_truthy  and dag_run.conf.assignedshift == "1" }}''',
            yes_task="put_user_payroll_rate_schedule_initial_schedule_85",
            no_task="if_request_hourlyrate2_present_86",
        )

        put_user_payroll_rate_schedule_initial_schedule_85 = rail.RepliconServiceOperator(
            task_id='put_user_payroll_rate_schedule_initial_schedule_85',
            endpoint="/services/PayrollService1.svc/PutUserPayrollRateSchedule",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_34')['uri'],
                "schedule": {
                    "initialHourlyRate": {
                        "amount": dag_run.conf['hourlyratejobdata'],
                        "currency": {
                            "uri": rail.result('get_base_currency_details_83')['uri'],
                            "name": null,
                            "symbol": null
                        }
                    },
                    "scheduleEntries": []
                }
            }
        )

        if_request_hourlyrate2_present_86 = rail.IfOperator(
            task_id='if_request_hourlyrate2_present_86',
            test='''{{ dag_run.conf.hourlyrate2 | is_truthy  and dag_run.conf.assignedshift != "1" }}''',
            yes_task="put_initial_payrollrate_schedule_with_effectivedate_for_houly_rate2_87",
            no_task="if_request_eestatus_equals_to_a_88",
        )

        put_initial_payrollrate_schedule_with_effectivedate_for_houly_rate2_87 = rail.RepliconServiceOperator(
            task_id='put_initial_payrollrate_schedule_with_effectivedate_for_houly_rate2_87',
            endpoint="/services/PayrollService1.svc/PutUserPayrollRateSchedule",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_34')['uri'],
                "schedule": {
                    "initialHourlyRate": {
                        "amount": dag_run.conf['hourlyratejobdata'],
                        "currency": {
                            "uri": rail.result('get_base_currency_details_83')['uri'],
                            "name": null,
                            "symbol": null
                        }
                    },
                    "scheduleEntries": [
                        {
                            "hourlyRate": {
                                "amount": dag_run.conf['hourlyrate2'],
                                "currency": {
                                    "uri": rail.result('get_base_currency_details_83')['uri'],
                                    "name": null,
                                    "symbol": null
                                }
                            },
                            "effectiveDate": {
                                "year": rail.result('invoke_custom_ruby_code_todays_date_11')['year'],
                                "month": rail.result('invoke_custom_ruby_code_todays_date_11')['month'],
                                "day": rail.result('invoke_custom_ruby_code_todays_date_11')['day']
                            }
                        }
                    ]
                }
            }
        )

        if_request_eestatus_equals_to_a_88 = rail.IfOperator(
            task_id='if_request_eestatus_equals_to_a_88',
            test='''{{ dag_run.conf.eestatus == 'A' }}''',
            yes_task="if_request_fullparttime_present_89",
            no_task="fdt_user_import_logs_add_entry_93",
        )

        if_request_fullparttime_present_89 = rail.IfOperator(
            task_id='if_request_fullparttime_present_89',
            test='''{{ dag_run.conf.fullparttime | is_truthy  and dag_run.conf.regulartemporary | is_truthy }}''',
            yes_task="trigger_dag_run_live_fdt_child_workflow_to_add_timeoff_type_for_new_user_v3_090",
            no_task="log_errorfornotassigningtimeoff_92",
        )

        trigger_dag_run_live_fdt_child_workflow_to_add_timeoff_type_for_new_user_v3_090 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_fdt_child_workflow_to_add_timeoff_type_for_new_user_v3_090',
            retries=0,
            trigger_dag_id=f'fujifilmdbtl_child_add_timeoff_type_for_new_user_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf['loginname'],
                "useruri": rail.result('create_user_34')['uri'],
                "startdatemonth": datetime.strptime(dag_run.conf['servicedate'], "%m/%d/%Y").strftime("%b"),
                "ftpt": dag_run.conf['fullparttime'].lower(),
                "regulartemp": dag_run.conf['regulartemporary']
            }
        )

        wait_for_completion_trigger_dag_run_live_fdt_child_workflow_to_add_timeoff_type_for_new_user_v3_090 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_fdt_child_workflow_to_add_timeoff_type_for_new_user_v3_090',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_fdt_child_workflow_to_add_timeoff_type_for_new_user_v3_090") }}'
        )

        log_errorfornotassigningtimeoff_92 = rail.PythonOperator(
            task_id='log_errorfornotassigningtimeoff_92',
            python_callable=lambda dag_run: rail.smartjoin_by_delim("Timeoff not assigned as " + ("" if dag_run.conf['fullparttime'] else "Full/Part Time field is blank") + "," + (
                "" if dag_run.conf['regulartemporary'] else "Regular/Temporary field is blank").split(","), ",")
        )

        fdt_user_import_logs_add_entry_93 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entry_93',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="na",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "username": rail.render_template("{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"),
                "loginname": dag_run.conf['loginname'],
                "emplid": dag_run.conf['emplid'],
                "action": "Add",
                "status": "Exception" if rail.result('log_errorwhenuserandsupervisorsloginnamearesame_73') else (
                    "Exception" if rail.result('log_error_logfordepartmentcodenotpresent_27') else (
                        "Exception" if rail.result('log_errorforsupervisorassignment_53') else (
                            "Exception" if rail.result('log_errorfornotassigningtimeoff_92') else (
                                "Exception" if rail.result('log_logforincorrectrehiredateformat_76') else "Success")))),
                "details": rail.smartjoin_by_delim(("Added Successfully" + "," + str(
                    rail.result('log_errorwhenuserandsupervisorsloginnamearesame_73') or "") + "," + str(
                        rail.result('log_logforincorrectrehiredateformat_76') or "") + "," + str(
                            rail.result('log_errorforsupervisorassignment_53') or "") + "," + str(
                                rail.result('log_error_logfordepartmentcodenotpresent_27') or "") + "," + str(
                                    rail.result('log_errorfornotassigningtimeoff_92') or "")).split(","), ";"),
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.userimportlogtable }}",
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "username": rail.render_template("{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"),
                "loginname": dag_run.conf['loginname'],
                "emplid": dag_run.conf['emplid'],
                "action": "Add",
                "status": "Error",
                "details": rail.render_template('{{ get_error_message() }}'),
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> if_request_servicedate_blank_3
        if_request_servicedate_blank_3 >> rail.Label(
            'Yes') >> fdt_user_import_logs_add_entry_4 >> catch_and_log_errors
        if_request_servicedate_blank_3 >> rail.Label(
            'No') >> declare_variable_6 >> if_request_eetype_blank_7
        if_request_eetype_blank_7 >> rail.Label(
            'Yes') >> fdt_user_import_logs_add_entry_8 >> catch_and_log_errors
        if_request_eetype_blank_7 >> rail.Label(
            'No') >> invoke_custom_ruby_code_service_date_10 >> invoke_custom_ruby_code_todays_date_11 >> if_request_eetype_equals_to_s_12
        if_request_eetype_equals_to_s_12 >> rail.Label(
            'Yes') >> update_variable_13 >> if_request_eetype_equals_to_h_14
        if_request_eetype_equals_to_s_12 >> rail.Label(
            'No') >> if_request_eetype_equals_to_h_14
        if_request_eetype_equals_to_h_14 >> rail.Label(
            'Yes') >> update_variable_15 >> _adhoc_http_action_16
        if_request_eetype_equals_to_h_14 >> rail.Label(
            'No') >> _adhoc_http_action_16 >> log_employee_type_uri_17 >> if_log_employee_type_uri_17_blank_18
        if_log_employee_type_uri_17_blank_18 >> rail.Label(
            'Yes') >> fdt_user_import_logs_add_entry_19 >> catch_and_log_errors
        if_log_employee_type_uri_17_blank_18 >> rail.Label(
            'No') >> _adhoc_http_action_21 >> if_request_deptid_present_22
        if_request_deptid_present_22 >> rail.Label(
            'Yes') >> get_datafordepartmentbasedonnameandcode_23 >> invoke_custom_ruby_code_24 >> log_departmenturi_25 >> log_requireddepartment_28
        if_request_deptid_present_22 >> rail.Label(
            'No') >> log_error_logfordepartmentcodenotpresent_27 >> log_requireddepartment_28 >> _adhoc_http_action_29 >> log_biweekly_wages_30 >> declare_variable_31 >> if_request_fullparttime_not_equals_to_f_32
        if_request_fullparttime_not_equals_to_f_32 >> rail.Label(
            'Yes') >> update_variable_33 >> create_user_34
        if_request_fullparttime_not_equals_to_f_32 >> rail.Label(
            'No') >> create_user_34 >> remove_timeoffassignmentsforusers_35 >> if_request_regulartemporary_equals_to_t_36
        if_request_regulartemporary_equals_to_t_36 >> rail.Label(
            'Yes') >> remove_holiday_calendarassignmentsforusers_37 >> log_service_date_u_d_furi_38
        if_request_regulartemporary_equals_to_t_36 >> rail.Label(
            'No') >> log_service_date_u_d_furi_38 >> if_log_service_date_u_d_furi_38_present_39
        if_log_service_date_u_d_furi_38_present_39 >> rail.Label(
            'Yes') >> update_service_date_u_d_f_40 >> log_adjusted_service_date_u_d_furi_41
        if_log_service_date_u_d_furi_38_present_39 >> rail.Label(
            'No') >> log_adjusted_service_date_u_d_furi_41 >> if_log_adjusted_service_date_u_d_furi_41_present_42
        if_log_adjusted_service_date_u_d_furi_41_present_42 >> rail.Label(
            'Yes') >> update_adjusted_service_date_u_d_f_43 >> log_getrequiredpayrule_44
        if_log_adjusted_service_date_u_d_furi_41_present_42 >> rail.Label(
            'No') >> log_getrequiredpayrule_44 >> _adhoc_http_action_45 >> log_get_pay_rule_script_uri_46 >> if_log_get_pay_rule_script_uri_46_present_enabled_47
        if_log_get_pay_rule_script_uri_46_present_enabled_47 >> rail.Label(
            'Yes') >> put_payroll_assignment_48 >> if_request_managerid_present_49
        if_log_get_pay_rule_script_uri_46_present_enabled_47 >> rail.Label(
            'No') >> if_request_managerid_present_49
        if_request_managerid_present_49 >> rail.Label(
            'Yes') >> search_users_50 >> user_search_result_list >> log_getthenumberofuserswithsameemployeeid_51 >> if_log_getthenumberofuserswithsameemployeeid_51_present_1_52
        if_log_getthenumberofuserswithsameemployeeid_51_present_1_52 >> rail.Label(
            'Yes') >> log_errorforsupervisorassignment_53 >> if_request_rehiredate_present_74
        if_log_getthenumberofuserswithsameemployeeid_51_present_1_52 >> rail.Label(
            'No') >> log_getsupervisor_uri_55 >> log_getsupervisorloginname_56 >> _adhoc_http_action_57 >> if_request_loginname_not_equals_to_dataloggerlog_getsupervisorloginname_56message_58
        if_request_loginname_not_equals_to_dataloggerlog_getsupervisorloginname_56message_58 >> rail.Label(
            'Yes') >> if_log_getsupervisor_uri_55_present_59
        if_request_loginname_not_equals_to_dataloggerlog_getsupervisorloginname_56message_58 >> rail.Label(
            'No') >> log_errorwhenuserandsupervisorsloginnamearesame_73 >> if_request_rehiredate_present_74
        if_log_getsupervisor_uri_55_present_59 >> rail.Label(
            'Yes') >> log_get_supervisor_status_60 >> if_log_get_supervisor_status_60_equals_to_true_61
        if_log_get_supervisor_status_60_equals_to_true_61 >> rail.Label(
            'Yes') >> _adhoc_http_action_62 >> log_checkifsupervisorhassupervisorpermission_63 >> if_log_checkifsupervisorhassupervisorpermission_63_blank_64
        if_log_get_supervisor_status_60_equals_to_true_61 >> rail.Label(
            'No') >> fdt_supervisor_assignment_table_add_entry_69 >> if_log_getsupervisor_uri_55_blank_70
        if_log_checkifsupervisorhassupervisorpermission_63_blank_64 >> rail.Label(
            'Yes') >> log_get_supervisor_permission_65 >> assign_supervsior_permission_set_to_user_66 >> update_initial_supervisor_67
        if_log_checkifsupervisorhassupervisorpermission_63_blank_64 >> rail.Label(
            'No') >> update_initial_supervisor_67 >> if_log_getsupervisor_uri_55_blank_70
        if_log_getsupervisor_uri_55_present_59 >> rail.Label(
            'No') >> if_log_getsupervisor_uri_55_blank_70
        if_log_getsupervisor_uri_55_blank_70 >> rail.Label(
            'Yes') >> fdt_supervisor_assignment_table_add_entry_71 >> if_request_rehiredate_present_74
        if_log_getsupervisor_uri_55_blank_70 >> rail.Label(
            'No') >> if_request_rehiredate_present_74
        if_request_managerid_present_49 >> rail.Label(
            'No') >> if_request_rehiredate_present_74
        if_request_rehiredate_present_74 >> rail.Label(
            'Yes') >> if_request_rehiredate_not_contains_75
        if_request_rehiredate_not_contains_75 >> rail.Label(
            'Yes') >> log_logforincorrectrehiredateformat_76 >> get_base_currency_details_83
        if_request_rehiredate_not_contains_75 >> rail.Label(
            'No') >> invoke_custom_ruby_code_rehire_date_79 >> log_rehire_date_u_d_furi_80 >> if_log_rehire_date_u_d_furi_80_present_81
        if_log_rehire_date_u_d_furi_80_present_81 >> rail.Label(
            'Yes') >> update_rehire_date_u_d_f_82 >> get_base_currency_details_83
        if_log_rehire_date_u_d_furi_80_present_81 >> rail.Label(
            'No') >> get_base_currency_details_83
        if_request_rehiredate_present_74 >> rail.Label(
            'No') >> get_base_currency_details_83 >> if_request_hourlyratejobdata_present_84
        if_request_hourlyratejobdata_present_84 >> rail.Label(
            'Yes') >> put_user_payroll_rate_schedule_initial_schedule_85 >> if_request_hourlyrate2_present_86
        if_request_hourlyratejobdata_present_84 >> rail.Label(
            'No') >> if_request_hourlyrate2_present_86
        if_request_hourlyrate2_present_86 >> rail.Label(
            'Yes') >> put_initial_payrollrate_schedule_with_effectivedate_for_houly_rate2_87 >> if_request_eestatus_equals_to_a_88
        if_request_hourlyrate2_present_86 >> rail.Label(
            'No') >> if_request_eestatus_equals_to_a_88
        if_request_eestatus_equals_to_a_88 >> rail.Label(
            'Yes') >> if_request_fullparttime_present_89
        if_request_fullparttime_present_89 >> rail.Label(
            'Yes') >> trigger_dag_run_live_fdt_child_workflow_to_add_timeoff_type_for_new_user_v3_090 >> wait_for_completion_trigger_dag_run_live_fdt_child_workflow_to_add_timeoff_type_for_new_user_v3_090 >> fdt_user_import_logs_add_entry_93
        if_request_fullparttime_present_89 >> rail.Label(
            'No') >> log_errorfornotassigningtimeoff_92 >> fdt_user_import_logs_add_entry_93
        if_request_eestatus_equals_to_a_88 >> rail.Label(
            'No') >> fdt_user_import_logs_add_entry_93 >> catch_and_log_errors >> finish

    return dag


rail.for_each_instance(create_dag)

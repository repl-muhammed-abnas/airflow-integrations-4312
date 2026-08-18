
from datetime import timedelta, datetime
import itertools
import pendulum
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'genoadesign_user_import_gdi_child_add_user_v1_0_{config.instance}',
        description=f'Live|GDI_Child_Add User V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=config.schedule_interval,
        max_active_runs=config.child_dag_max_active_runs,
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
            no_task='if_request_loginstatus_not_equals_to_enabled_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_loginstatus_not_equals_to_enabled_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_loginstatus_not_equals_to_enabled_3 = rail.IfOperator(
            task_id='if_request_loginstatus_not_equals_to_enabled_3',
            test='''{{ dag_run.conf.loginstatus != 'Enabled' }}''',
            yes_task="genoadi_user_import_logs_add_entry_4",
            no_task="if_request_loginname_blank_enabled_6",
        )

        def get_login_status(dag_run):
            details = "Login Status is not present"
            if dag_run.conf['loginstatus'] != 'Enabled':
                details = "Login status is not enabled"
            return details

        genoadi_user_import_logs_add_entry_4 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_add_entry_4',
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "username|loginname": dag_run.conf['firstname'] + dag_run.conf['lastname'] + "|" + dag_run.conf['loginname'],
                "status": "Skipped",
                "details": get_login_status(dag_run),
                "childjobid": get_dagrun_ecid(dag_run)
            }
        )

        if_request_loginname_blank_enabled_6 = rail.IfOperator(
            task_id='if_request_loginname_blank_enabled_6',
            test='''{{ dag_run.conf.loginname | is_falsy }}''',
            yes_task="genoadi_user_import_logs_add_entry_7",
            no_task="log_todaysdate_9",
        )

        genoadi_user_import_logs_add_entry_7 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_add_entry_7',
            message="na",
            severity="Skipped",
            properties={
                "username|loginname": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} |{{ dag_run.conf.loginname }}",
                "status": "Skipped",
                "details": "Login Name is not present",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_todaysdate_9 = rail.PythonOperator(
            task_id='log_todaysdate_9',
            python_callable=lambda:  pendulum.now(
                config.pacific_timezone).strftime('%m/%d/%Y')
        )

        _adhoc_http_action_16 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_16',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data=None
        )

        def get_employee_type_uri(dag_run):
            current_employeetype = list(filter(lambda x: x['name'] and x['name'].lower().replace(
                '-', ' ') == dag_run.conf['employeetype'], rail.result('_adhoc_http_action_16')))
            return current_employeetype[0]['uri'] if current_employeetype else None

        log_employee_type_uri_19 = rail.PythonOperator(
            task_id='log_employee_type_uri_19',
            python_callable=get_employee_type_uri
        )

        if_log_employee_type_uri_19_blank_20 = rail.IfOperator(
            task_id='if_log_employee_type_uri_19_blank_20',
            test='''{{ result('log_employee_type_uri_19') | is_falsy }}''',
            yes_task="genoadi_user_import_logs_add_entry_21",
            no_task="create_user_23",
        )

        genoadi_user_import_logs_add_entry_21 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_add_entry_21',
            message="na",
            severity="Exception",
            properties={
                "username|loginname": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} |{{ dag_run.conf.loginname }}",
                "status": "Exception",
                "details": "Required employee type is not present in Replicon",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        create_user_23 = rail.RepliconServiceOperator(
            task_id='create_user_23',
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
                    "employeeId": dag_run.conf['employeeid'],
                    "department": {
                        "uri": null,
                        "name": "Genoa Design",
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": "Genoa Design - 8 hours/day; Mon-Fri",
                                "officeSchedule": null,
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "workWeekStartDayUri": "urn:replicon:day-of-week:saturday",
                    "employmentDateRange": {
                        "startDate": {
                            "year": datetime.strptime(dag_run.conf['startdate'], '%Y%m%d').year,
                            "month": datetime.strptime(dag_run.conf['startdate'], '%Y%m%d').month,
                            "day": datetime.strptime(dag_run.conf['startdate'], '%Y%m%d').day,
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
                            "name": "Project Resource"
                        }
                    ],
                    "policySets": [
                        {
                            "uri": null,
                            "name": "Standard Timesheet - Genoa Design International"
                        },
                        {
                            "uri": null,
                            "name": "Time Off"
                        }
                    ],
                    "employeeType": {
                        "uri": rail.result('log_employee_type_uri_19'),
                        "name": null
                    },
                    "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:system",
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {
                        "uri": null,
                        "name": "Project Manager"
                    },
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": {
                        "uri": null,
                        "name": "Supervisor"
                    },
                    "customFieldValues": [],
                    "assignedActivities": [],
                    "timeZone": null,
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

        remove_timeoffassignmentsforusers_24 = rail.RepliconServiceOperator(
            task_id='remove_timeoffassignmentsforusers_24',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_23').uri }}",
                "timeOffTypeUris": []
            }
        )

        _adhoc_http_action_25 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_25',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data=None
        )

        def get_department_uri(dag_run):
            if dag_run.conf['departmentname']:
                current_department = list(filter(lambda x: x['name'] and x['displayText'].lower(
                ) == dag_run.conf['departmentname'].lower(), rail.result('_adhoc_http_action_25')))
                return current_department[0]['uri'] if current_department else None
            return None

        log_departmenturi_26 = rail.PythonOperator(
            task_id='log_departmenturi_26',
            python_callable=get_department_uri
        )

        if_log_departmenturi_26_present_27 = rail.IfOperator(
            task_id='if_log_departmenturi_26_present_27',
            test='''{{ result('log_departmenturi_26') | is_truthy }}''',
            yes_task="update_department_for_user_28",
            no_task="log_error_logfordepartmentnotpresent_30",
        )

        update_department_for_user_28 = rail.RepliconServiceOperator(
            task_id='update_department_for_user_28',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ result('create_user_23').uri }}",
                "departmentUri": "{{ result('log_departmenturi_26') }}"
            }
        )

        log_error_logfordepartmentnotpresent_30 = rail.PythonOperator(
            task_id='log_error_logfordepartmentnotpresent_30',
            python_callable=lambda dag_run:  "Department not added for User " + dag_run.conf['firstname'] + " " + dag_run.conf['lastname'] + "." +
            dag_run.conf['departmentname'] +
                    "is not available in Replicon, hence, 'Genoa Design' is added as department for the user;"
        )

        log_pluckif_pay_ruleispresent_31 = rail.PythonOperator(
            task_id='log_pluckif_pay_ruleispresent_31',
            python_callable=lambda dag_run: "Genoa Design - Overtime rule" if "full time hourly" in dag_run.conf[
                'employeetype'] else "No Payrule"
        )

        _adhoc_http_action_32 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_32',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data=None
        )

        log_get_pay_rule_script_uri_33 = rail.PythonOperator(
            task_id='log_get_pay_rule_script_uri_33',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_32'), 'displayText', rail.result('log_pluckif_pay_ruleispresent_31'), 'uri')
        )

        if_log_get_pay_rule_script_uri_33_present_enabled_34 = rail.IfOperator(
            task_id='if_log_get_pay_rule_script_uri_33_present_enabled_34',
            test='''{{ result('log_get_pay_rule_script_uri_33') | is_truthy }}''',
            yes_task="put_payroll_assignment_35",
            no_task="get_activity_dataforthedepartment_36",
        )

        put_payroll_assignment_35 = rail.RepliconServiceOperator(
            task_id='put_payroll_assignment_35',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ result('create_user_23').uri }}",
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": "{{ result('log_get_pay_rule_script_uri_33') }}",
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def compose_user_details(response, department):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['code'] == department, map(lambda row: {
                'name': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
                'uri': row['cells'][0]['uri'],
                'code': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None
            }, flaten_rows)))
            return users_info[0] if users_info else None

        get_activity_dataforthedepartment_36 = rail.RepliconServicePageOperator(
            task_id='get_activity_dataforthedepartment_36',
            endpoint="/services/ActivityListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 1000000,
                "columnUris": [
                    "urn:replicon:activity-list-column:activity",
                    "urn:replicon:activity-list-column:code"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:activity-list-filter:text"
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
                            "text": "{{ dag_run.conf.departmentname }}",
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
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['departmentname'])
        )

        if_log_activity_uristobeassigned_39_present_40 = rail.IfOperator(
            task_id='if_log_activity_uristobeassigned_39_present_40',
            test='''{{ result('get_activity_dataforthedepartment_36') | is_truthy }}''',
            yes_task="put_activity_assignments_for_user_41",
            no_task="if_request_supervisor_present_42",
        )

        put_activity_assignments_for_user_41 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_41',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_23').uri }}",
                "activityUris": [
                    "{{ result('get_activity_dataforthedepartment_36').uri }}"
                ]
            }
        )

        if_request_supervisor_present_42 = rail.IfOperator(
            task_id='if_request_supervisor_present_42',
            test='''{{ dag_run.conf.supervisor | is_truthy }}''',
            yes_task="search_users_43",
            no_task="if_request_employeehourlycost_present_63",
        )

        def compose_super_details(response, supervisor):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['loginname'] == supervisor, map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))
            return users_info[0] if users_info else None

        search_users_43 = rail.RepliconServicePageOperator(
            task_id="search_users_43",
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
                            'text': dag_run.conf['supervisor'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_super_details(
                response, dag_run.conf['supervisor'])
        )

        _adhoc_http_action_46 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_46',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        if_request_loginname_not_equals_to_dataloggerlog_getsupervisor_login_name_45message_47 = rail.IfOperator(
            task_id='if_request_loginname_not_equals_to_dataloggerlog_getsupervisor_login_name_45message_47',
            test='''{{ result('search_users_43') | is_truthy and dag_run.conf.loginname != result('search_users_43').loginname }}''',
            yes_task="if_log_getsupervisor_uri_44_present_48",
            no_task="log_errorwhenuserandsupervisorsloginnamearesame_62",
        )

        if_log_getsupervisor_uri_44_present_48 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_44_present_48',
            test='''{{ result('search_users_43').useruri | is_truthy }}''',
            yes_task="if_log_get_supervisor_status_49_equals_to_true_50",
            no_task="if_log_getsupervisor_uri_44_blank_59",
        )

        if_log_get_supervisor_status_49_equals_to_true_50 = rail.IfOperator(
            task_id='if_log_get_supervisor_status_49_equals_to_true_50',
            test='''{{ result('search_users_43').status == 'True' }}''',
            yes_task="_adhoc_http_action_51",
            no_task="genoadi_supervisor_assignment_table_add_entry_58",
        )

        _adhoc_http_action_51 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_51',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_43').useruri }}"
            }
        )

        def get_supervision_permission(permission_task):
            permissionset = rail.find_first_by_attr_and_get_attr(rail.result(
                permission_task), 'policyUri', "urn:replicon:policy:supervision", 'permissionSet')
            return permissionset['uri'] if permissionset else None

        log_checkifsupervisorhassupervisorpermission_52 = rail.PythonOperator(
            task_id='log_checkifsupervisorhassupervisorpermission_52',
            python_callable=lambda: get_supervision_permission(
                '_adhoc_http_action_51')
        )

        if_log_checkifsupervisorhassupervisorpermission_52_blank_53 = rail.IfOperator(
            task_id='if_log_checkifsupervisorhassupervisorpermission_52_blank_53',
            test='''{{ result('log_checkifsupervisorhassupervisorpermission_52') | is_falsy }}''',
            yes_task="log_get_supervisor_permission_54",
            no_task="update_initial_supervisor_56",
        )

        log_get_supervisor_permission_54 = rail.PythonOperator(
            task_id='log_get_supervisor_permission_54',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_46'), 'displayText', "Supervisor", 'uri')
        )

        assign_supervsior_permission_set_to_user_55 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_55',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_43').useruri }}",
                "permissionSetUri": "{{ result('log_get_supervisor_permission_54') }}"
            }
        )

        update_initial_supervisor_56 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_56',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ result('create_user_23').uri }}",
                "initialSupervisorUri": "{{ result('search_users_43').useruri }}",
                "scheduleEntries": []
            }
        )

        genoadi_supervisor_assignment_table_add_entry_58 = rail.WriteLogOperator(
            task_id='genoadi_supervisor_assignment_table_add_entry_58',
            message="na",
            log="{{ dag_run.conf.supervisor_processing_log }}",
            severity="Success",
            properties={
                "userloginname": "{{ dag_run.conf.loginname }}|{{ result('create_user_23').uri }}|{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorloginname": "{{ dag_run.conf.supervisor }}|{{ dag_run.conf.supervisoreffectivedate }}",
                "action": "Add"
            }
        )

        if_log_getsupervisor_uri_44_blank_59 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_44_blank_59',
            test='''{{ result('search_users_43') | is_falsy }}''',
            yes_task="genoadi_supervisor_assignment_table_add_entry_60",
            no_task="if_request_employeehourlycost_present_63",
        )

        genoadi_supervisor_assignment_table_add_entry_60 = rail.WriteLogOperator(
            task_id='genoadi_supervisor_assignment_table_add_entry_60',
            message="na",
            log="{{ dag_run.conf.supervisor_processing_log }}",
            severity="Success",
            properties={
                "userloginname": "{{ dag_run.conf.loginname }}|{{ result('create_user_23').uri }}|{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorloginname": "{{ dag_run.conf.supervisor }}|{{ dag_run.conf.supervisoreffectivedate }}",
                "action": "Add"
            }
        )

        log_errorwhenuserandsupervisorsloginnamearesame_62 = rail.PythonOperator(
            task_id='log_errorwhenuserandsupervisorsloginnamearesame_62',
            python_callable=lambda dag_run: "User" + dag_run.conf['firstname'] + " " + dag_run.conf['lastname'] +
            " is created, however supervisor is not updated as the 'Login name' for user and supervisor is same;"
        )

        if_request_employeehourlycost_present_63 = rail.IfOperator(
            task_id='if_request_employeehourlycost_present_63',
            test='''{{ dag_run.conf.employeehourlycost | is_truthy }}''',
            yes_task="if_request_userhourlycostcurrency_present_64",
            no_task="if_request_timezone_present_72",
        )

        if_request_userhourlycostcurrency_present_64 = rail.IfOperator(
            task_id='if_request_userhourlycostcurrency_present_64',
            test='''{{ dag_run.conf.userhourlycostcurrency | is_truthy }}''',
            yes_task="_adhoc_http_action_65",
            no_task="_adhoc_http_action_68",
        )

        _adhoc_http_action_65 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_65',
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
            data=None
        )

        log_get_currency_uri_66 = rail.PythonOperator(
            task_id='log_get_currency_uri_66',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_65'), 'displayText', dag_run.conf['userhourlycostcurrency'], 'uri')
        )

        _adhoc_http_action_68 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_68',
            endpoint="/services/CurrencyService2.svc/GetBaseCurrency",
            data=None
        )

        log_get_currency_uri_69 = rail.PythonOperator(
            task_id='log_get_currency_uri_69',
            python_callable=lambda:  "{{ result('_adhoc_http_action_68').uri }}"
        )

        log_required_currency_uri_70 = rail.PythonOperator(
            task_id='log_required_currency_uri_70',
            python_callable=lambda:  rail.result(
                'log_get_currency_uri_66') or rail.result('log_get_currency_uri_69')
        )

        put_user_hourly_cost_schedule_initial_schedule_71 = rail.RepliconServiceOperator(
            task_id='put_user_hourly_cost_schedule_initial_schedule_71',
            endpoint="/services/ResourceService1.svc/UpdateUserCostRateScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_23').uri }}",
                "hourlyRate": {
                    "amount": "{{ dag_run.conf.employeehourlycost }}",
                    "currencyUri": "{{ result('log_required_currency_uri_70') }}"
                },
                "dateRange": null
            }
        )

        if_request_timezone_present_72 = rail.IfOperator(
            task_id='if_request_timezone_present_72',
            test='''{{ dag_run.conf.timezone | is_truthy }}''',
            yes_task="_adhoc_http_action_80",
            no_task="if_request_holidaycalendar_present_84",
        )

        _adhoc_http_action_80 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_80',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
            data=None
        )

        def get_timezone_uri(dag_run):
            timezone = ""
            if dag_run.conf['timezone'] == 'NL':
                timezone = "(UTC-3:30) Newfoundland Standard Time"
            if dag_run.conf['timezone'] == 'BC':
                timezone = "(UTC-8:00) Pacific Standard Time"
            if dag_run.conf['timezone'] == 'New Orleans':
                timezone = "(UTC-6:00) Central Standard Time"
            timezone_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_80'), 'displayText', timezone, 'uri')
            return timezone_uri

        log_get_time_zone_uri_81 = rail.PythonOperator(
            task_id='log_get_time_zone_uri_81',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda dag_run: get_timezone_uri(dag_run)
        )

        if_log_get_time_zone_uri_81_present_82 = rail.IfOperator(
            task_id='if_log_get_time_zone_uri_81_present_82',
            test='''{{ result('log_get_time_zone_uri_81') | is_truthy }}''',
            yes_task="update_time_zone_for_user_83",
            no_task="if_request_holidaycalendar_present_84",
        )

        update_time_zone_for_user_83 = rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user_83',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ result('create_user_23').uri }}",
                "timeZoneUri": "{{ result('log_get_time_zone_uri_81') }}"
            }
        )

        if_request_holidaycalendar_present_84 = rail.IfOperator(
            task_id='if_request_holidaycalendar_present_84',
            test='''{{ dag_run.conf.holidaycalendar | is_truthy }}''',
            yes_task="_adhoc_http_action_85",
            no_task="if_request_location_present_89",
        )

        _adhoc_http_action_85 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_85',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data=None
        )

        log_get_holiday_calendar_uri_86 = rail.PythonOperator(
            task_id='log_get_holiday_calendar_uri_86',
            python_callable=lambda dag_run:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_85'), 'displayText', dag_run.conf['holidaycalendar'], 'uri')
        )

        if_log_get_holiday_calendar_uri_86_present_87 = rail.IfOperator(
            task_id='if_log_get_holiday_calendar_uri_86_present_87',
            test='''{{ result('log_get_holiday_calendar_uri_86') | is_truthy }}''',
            yes_task="update_holiday_calendar_for_user_88",
            no_task="if_request_location_present_89",
        )

        update_holiday_calendar_for_user_88 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user_88',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_user_23').uri }}",
                "holidayCalendarUri": "{{ result('log_get_holiday_calendar_uri_86') }}"
            }
        )

        if_request_location_present_89 = rail.IfOperator(
            task_id='if_request_location_present_89',
            test='''{{ dag_run.conf.location | is_truthy }}''',
            yes_task="_adhoc_http_action_90",
            no_task="if_request_team_present_96",
        )

        _adhoc_http_action_90 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_90',
            endpoint="/services/LocationService1.svc/GetAllLocations",
            data=None
        )

        log_get_required_location_uri_91 = rail.PythonOperator(
            task_id='log_get_required_location_uri_91',
            python_callable=lambda dag_run:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_90'), 'displayText', dag_run.conf['location'], 'uri')
        )

        if_log_get_required_location_uri_91_present_92 = rail.IfOperator(
            task_id='if_log_get_required_location_uri_91_present_92',
            test='''{{ result('log_get_required_location_uri_91') | is_truthy }}''',
            yes_task="put_location_schedule_for_user_93",
            no_task="log_errormessageincasewhenlocationisnotavailable_95",
        )

        put_location_schedule_for_user_93 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_93',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('create_user_23').uri }}",
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": "{{ result('log_get_required_location_uri_91') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_errormessageincasewhenlocationisnotavailable_95 = rail.PythonOperator(
            task_id='log_errormessageincasewhenlocationisnotavailable_95',
            python_callable=lambda dag_run: "Location not added for User " +
            dag_run.conf['firstname'] + " " + dag_run.conf['lastname'] +
                    " as " + dag_run.conf['location'] +
            " is not available in Replicon"
        )

        if_request_team_present_96 = rail.IfOperator(
            task_id='if_request_team_present_96',
            test='''{{ dag_run.conf.team | is_truthy }}''',
            yes_task="_adhoc_http_action_97",
            no_task="if_request_loginstatus_equals_to_enabled_103",
        )

        _adhoc_http_action_97 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_97',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data=None
        )

        log_get_required_team_uri_98 = rail.PythonOperator(
            task_id='log_get_required_team_uri_98',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_97'), 'displayText', dag_run.conf['team'], 'uri')
        )

        if_log_get_required_team_uri_98_present_99 = rail.IfOperator(
            task_id='if_log_get_required_team_uri_98_present_99',
            test='''{{ result('log_get_required_team_uri_98') | is_truthy }}''',
            yes_task="put_cost_center_schedule_for_user_100",
            no_task="log_errormessageincasewhenteamisnotavailable_102",
        )

        put_cost_center_schedule_for_user_100 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_100',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": "{{ result('create_user_23').uri }}",
                "scheduleEntries": [
                    {
                        "costCenter": {
                            "uri": "{{ result('log_get_required_team_uri_98') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_errormessageincasewhenteamisnotavailable_102 = rail.PythonOperator(
            task_id='log_errormessageincasewhenteamisnotavailable_102',
            python_callable=lambda dag_run: "Team not added for User " +
            dag_run.conf['firstname'] + " " + dag_run.conf['lastname'] +
                    " as " + dag_run.conf['team'] +
            " is not available in Replicon"
        )

        if_request_loginstatus_equals_to_enabled_103 = rail.IfOperator(
            task_id='if_request_loginstatus_equals_to_enabled_103',
            test='''{{ dag_run.conf.loginstatus | lower == 'enabled' }}''',
            yes_task="trigger_dag_run_live_gdi_child_workflow_to_add_timeoff_type_for_new_user_v1_0104",
            no_task="genoadi_user_import_logs_add_entry_105",
        )

        trigger_dag_run_live_gdi_child_workflow_to_add_timeoff_type_for_new_user_v1_0104 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_gdi_child_workflow_to_add_timeoff_type_for_new_user_v1_0104',
            retries=0,
            items=[-1],
            trigger_dag_id=f'genoadesign_user_import_gdi_child_workflow_to_add_timeoff_type_for_new_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "parentjobid": "{{ dag_run_ecid() }}",
                "userloginname": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ result('create_user_23').uri }}",
                "employeetype": "{{ dag_run.conf.employeetype }}"
            }
        )

        wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_add_timeoff_type_for_new_user_v1_0104 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_add_timeoff_type_for_new_user_v1_0104',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_gdi_child_workflow_to_add_timeoff_type_for_new_user_v1_0104") }}'
        )

        def get_status_details():
            details = ["Added Successfully",
                       rail.result(
                           'log_errorwhenuserandsupervisorsloginnamearesame_62'),
                       rail.result('log_error_logfordepartmentnotpresent_30'),
                       rail.result(
                           'log_errormessageincasewhenlocationisnotavailable_95'),
                       rail.result(
                           'log_errormessageincasewhenteamisnotavailable_102')]
            return rail.smartjoin_by_delim(details, ';')

        genoadi_user_import_logs_add_entry_105 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_add_entry_105',
            message="na",
            severity=lambda: "Failed" if rail.result('log_errorwhenuserandsupervisorsloginnamearesame_62') or rail.result('log_error_logfordepartmentnotpresent_30') or rail.result(
                'log_errormessageincasewhenlocationisnotavailable_95') or rail.result('log_errormessageincasewhenteamisnotavailable_102') else "Success",
            properties=lambda dag_run: {
                "username|loginname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'] + "|" + dag_run.conf['loginname'],
                "status": "Failed" if rail.result('log_errorwhenuserandsupervisorsloginnamearesame_62') or rail.result('log_error_logfordepartmentnotpresent_30') or rail.result(
                    'log_errormessageincasewhenlocationisnotavailable_95') or rail.result('log_errormessageincasewhenteamisnotavailable_102') else "Success",
                "details": get_status_details(),
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        genoadi_user_import_logs_add_entry_107 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_add_entry_107',
            message="{{ get_error_message() }}",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "username|loginname": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}|{{ dag_run.conf.loginname }}",
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
        can_run_batch_task >> rail.Label(
            'No') >> if_request_loginstatus_not_equals_to_enabled_3
        if_request_loginstatus_not_equals_to_enabled_3
        if_request_loginstatus_not_equals_to_enabled_3 >> rail.Label(
            'Yes') >> genoadi_user_import_logs_add_entry_4 >> genoadi_user_import_logs_add_entry_107
        if_request_loginstatus_not_equals_to_enabled_3 >> rail.Label(
            'No') >> if_request_loginname_blank_enabled_6
        if_request_loginname_blank_enabled_6 >> rail.Label(
            'Yes') >> genoadi_user_import_logs_add_entry_7 >> genoadi_user_import_logs_add_entry_107
        if_request_loginname_blank_enabled_6 >> rail.Label(
            'No') >> log_todaysdate_9 >> _adhoc_http_action_16 >> log_employee_type_uri_19 >> if_log_employee_type_uri_19_blank_20
        if_log_employee_type_uri_19_blank_20 >> rail.Label(
            'Yes') >> genoadi_user_import_logs_add_entry_21 >> genoadi_user_import_logs_add_entry_107
        if_log_employee_type_uri_19_blank_20 >> rail.Label(
            'No') >> create_user_23 >> remove_timeoffassignmentsforusers_24 >> _adhoc_http_action_25 >> log_departmenturi_26 >> if_log_departmenturi_26_present_27
        if_log_departmenturi_26_present_27 >> rail.Label(
            'Yes') >> update_department_for_user_28 >> log_pluckif_pay_ruleispresent_31
        if_log_departmenturi_26_present_27 >> rail.Label(
            'No') >> log_error_logfordepartmentnotpresent_30 >> log_pluckif_pay_ruleispresent_31 >> _adhoc_http_action_32 >> \
            log_get_pay_rule_script_uri_33 >> if_log_get_pay_rule_script_uri_33_present_enabled_34
        if_log_get_pay_rule_script_uri_33_present_enabled_34 >> rail.Label(
            'Yes') >> put_payroll_assignment_35 >> get_activity_dataforthedepartment_36
        if_log_get_pay_rule_script_uri_33_present_enabled_34 >> rail.Label(
            'No') >> get_activity_dataforthedepartment_36 >> if_log_activity_uristobeassigned_39_present_40
        if_log_activity_uristobeassigned_39_present_40 >> rail.Label(
            'Yes') >> put_activity_assignments_for_user_41 >> if_request_supervisor_present_42
        if_log_activity_uristobeassigned_39_present_40 >> rail.Label(
            'No') >> if_request_supervisor_present_42
        if_request_supervisor_present_42 >> rail.Label(
            'Yes') >> search_users_43 >> _adhoc_http_action_46 >> if_request_loginname_not_equals_to_dataloggerlog_getsupervisor_login_name_45message_47
        if_request_loginname_not_equals_to_dataloggerlog_getsupervisor_login_name_45message_47 >> rail.Label(
            'No') >> log_errorwhenuserandsupervisorsloginnamearesame_62 >> if_request_employeehourlycost_present_63
        if_request_loginname_not_equals_to_dataloggerlog_getsupervisor_login_name_45message_47 >> rail.Label(
            'Yes') >> if_log_getsupervisor_uri_44_present_48
        if_log_getsupervisor_uri_44_present_48 >> rail.Label(
            'Yes') >> if_log_get_supervisor_status_49_equals_to_true_50
        if_log_get_supervisor_status_49_equals_to_true_50 >> rail.Label(
            'No') >> genoadi_supervisor_assignment_table_add_entry_58 >> if_log_getsupervisor_uri_44_blank_59
        if_log_get_supervisor_status_49_equals_to_true_50 >> rail.Label(
            'Yes') >> _adhoc_http_action_51 >> log_checkifsupervisorhassupervisorpermission_52 >> if_log_checkifsupervisorhassupervisorpermission_52_blank_53
        if_log_checkifsupervisorhassupervisorpermission_52_blank_53 >> rail.Label(
            'Yes') >> log_get_supervisor_permission_54 >> assign_supervsior_permission_set_to_user_55 >> update_initial_supervisor_56
        if_log_checkifsupervisorhassupervisorpermission_52_blank_53 >> rail.Label(
            'No') >> update_initial_supervisor_56 >> if_log_getsupervisor_uri_44_blank_59
        if_log_getsupervisor_uri_44_present_48 >> rail.Label(
            'No') >> if_log_getsupervisor_uri_44_blank_59
        if_log_getsupervisor_uri_44_blank_59 >> rail.Label(
            'Yes') >> genoadi_supervisor_assignment_table_add_entry_60 >> if_request_employeehourlycost_present_63
        if_log_getsupervisor_uri_44_blank_59 >> rail.Label(
            'No') >> if_request_employeehourlycost_present_63
        if_request_supervisor_present_42 >> rail.Label(
            'No') >> if_request_employeehourlycost_present_63
        if_request_employeehourlycost_present_63 >> rail.Label(
            'Yes') >> if_request_userhourlycostcurrency_present_64
        if_request_userhourlycostcurrency_present_64 >> rail.Label(
            'Yes') >> _adhoc_http_action_65 >> log_get_currency_uri_66 >> log_required_currency_uri_70
        if_request_userhourlycostcurrency_present_64 >> rail.Label(
            'No') >> _adhoc_http_action_68 >> log_get_currency_uri_69 >> log_required_currency_uri_70 >> \
            put_user_hourly_cost_schedule_initial_schedule_71 >> if_request_timezone_present_72
        if_request_employeehourlycost_present_63 >> rail.Label(
            'No') >> if_request_timezone_present_72
        if_request_timezone_present_72 >> rail.Label(
            'Yes') >> _adhoc_http_action_80 >> log_get_time_zone_uri_81 >> if_log_get_time_zone_uri_81_present_82
        if_log_get_time_zone_uri_81_present_82 >> rail.Label(
            'Yes') >> update_time_zone_for_user_83 >> if_request_holidaycalendar_present_84
        if_log_get_time_zone_uri_81_present_82 >> rail.Label(
            'No') >> if_request_holidaycalendar_present_84
        if_request_timezone_present_72 >> rail.Label(
            'No') >> if_request_holidaycalendar_present_84
        if_request_holidaycalendar_present_84 >> rail.Label(
            'Yes') >> _adhoc_http_action_85 >> log_get_holiday_calendar_uri_86 >> if_log_get_holiday_calendar_uri_86_present_87
        if_log_get_holiday_calendar_uri_86_present_87 >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user_88 >> if_request_location_present_89
        if_log_get_holiday_calendar_uri_86_present_87 >> rail.Label(
            'No') >> if_request_location_present_89
        if_request_holidaycalendar_present_84 >> rail.Label(
            'No') >> if_request_location_present_89
        if_request_location_present_89 >> rail.Label(
            'Yes') >> _adhoc_http_action_90 >> log_get_required_location_uri_91 >> if_log_get_required_location_uri_91_present_92
        if_log_get_required_location_uri_91_present_92 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_93 >> if_request_team_present_96
        if_log_get_required_location_uri_91_present_92 >> rail.Label(
            'No') >> log_errormessageincasewhenlocationisnotavailable_95 >> if_request_team_present_96
        if_request_location_present_89 >> rail.Label(
            'No') >> if_request_team_present_96
        if_request_team_present_96 >> rail.Label(
            'Yes') >> _adhoc_http_action_97 >> log_get_required_team_uri_98 >> if_log_get_required_team_uri_98_present_99
        if_log_get_required_team_uri_98_present_99 >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user_100 >> if_request_loginstatus_equals_to_enabled_103
        if_log_get_required_team_uri_98_present_99 >> rail.Label(
            'No') >> log_errormessageincasewhenteamisnotavailable_102 >> if_request_loginstatus_equals_to_enabled_103
        if_request_team_present_96 >> rail.Label(
            'No') >> if_request_loginstatus_equals_to_enabled_103
        if_request_loginstatus_equals_to_enabled_103 >> rail.Label(
            'Yes') >> trigger_dag_run_live_gdi_child_workflow_to_add_timeoff_type_for_new_user_v1_0104 >> \
            wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_add_timeoff_type_for_new_user_v1_0104 >> genoadi_user_import_logs_add_entry_105
        if_request_loginstatus_equals_to_enabled_103 >> rail.Label(
            'No') >> genoadi_user_import_logs_add_entry_105 >> genoadi_user_import_logs_add_entry_107 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)

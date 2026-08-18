
import itertools
from datetime import timedelta, datetime
import pendulum
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'intercontinentalexchange_child_adduser_v10_{config.instance}',
        description=f'IntercontinentalExchange_Child_Add User_V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
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
            no_task='search_users_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_users_4',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def all_result_data_handler(result, username):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], result))))
            existing_user = list(filter(lambda x: x['loginname'] == username, map(lambda row: {
                'username': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
                'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'loginname': row['cells'][1]['textValue'],
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))

            return existing_user[0] if existing_user else {}

        search_users_4 = rail.RepliconServicePageOperator(
            task_id="search_users_4",
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
                            'text': dag_run.conf['work_email']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result, dag_run: all_result_data_handler(
                result, dag_run.conf['work_email'])
        )

        if_login_name_textvalue_present_5 = rail.IfOperator(
            task_id='if_login_name_textvalue_present_5',
            test='''{{ result('search_users_4') | is_truthy }}''',
            yes_task="intercontinentalexchange_user_import_logs_add_entry_6",
            no_task="declare_list_8",
        )

        intercontinentalexchange_user_import_logs_add_entry_6 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_user_import_logs_add_entry_6',
            message="Exception",
            severity="Exception",
            properties={
                "Empid": "{{ dag_run.conf.employeeid }}",
                "Username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "Action": "Add",
                "Status": "Exception",
                "Details": "User {{dag_run.conf.employeeid}} not added as there is an existing user with login name:{{dag_run.conf.work_email}}",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        declare_list_8 = rail.SetVariableOperator(
            task_id='declare_list_8',
            append=False,
            name='exceptions',
            value=[]
        )

        declare_variable_10 = rail.SetVariableOperator(
            task_id='declare_variable_10',
            append=False,
            name='schedulePolicySchedule',
            value=None
        )

        declare_variable_11 = rail.SetVariableOperator(
            task_id='declare_variable_11',
            append=False,
            name='holidayCalendar',
            value=None
        )

        if_request_holidaycalendaruri_present_12 = rail.IfOperator(
            task_id='if_request_holidaycalendaruri_present_12',
            test='''{{ dag_run.conf.holidaycalendaruri | is_truthy }}''',
            yes_task="update_variable_13",
            no_task="insert_to_list_15",
        )

        update_variable_13 = rail.SetVariableOperator(
            task_id='update_variable_13',
            append=False,
            name='{{ result("declare_variable_11").name }}',
            value={
                "uri": "{{ dag_run.conf.holidaycalendaruri }}",
                "name": null
            }
        )

        insert_to_list_15 = rail.SetVariableOperator(
            task_id='insert_to_list_15',
            append=True,
            name='{{ result("declare_list_8").name }}',
            value={
                "log": "Holiday calendar not availble in Mapper or Replicon"
            }
        )

        get_all_permission_sets_16 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_16',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        declare_list_17 = rail.SetVariableOperator(
            task_id='declare_list_17',
            append=False,
            name='permissionSets',
            value=[]
        )

        declare_variable_18 = rail.SetVariableOperator(
            task_id='declare_variable_18',
            append=False,
            name='departmentGroupSchedule',
            value=None
        )

        if_request_department_present_19 = rail.IfOperator(
            task_id='if_request_department_present_19',
            test='''{{ dag_run.conf.department | is_truthy }}''',
            yes_task="update_variable_20",
            no_task="declare_variable_21",
        )

        update_variable_20 = rail.SetVariableOperator(
            task_id='update_variable_20',
            append=False,
            name='{{ result("declare_variable_18").name }}',
            value=[
                {
                    "departmentGroup": {
                        "uri": "{{ dag_run.conf.department }}",
                        "parent": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_21 = rail.SetVariableOperator(
            task_id='declare_variable_21',
            append=False,
            name='employeeTypeGroupSchedule',
            value=None
        )

        if_request_employeetypeuri_present_22 = rail.IfOperator(
            task_id='if_request_employeetypeuri_present_22',
            test='''{{ dag_run.conf.employeetypeuri | is_truthy }}''',
            yes_task="update_variable_23",
            no_task="declare_variable_24",
        )

        update_variable_23 = rail.SetVariableOperator(
            task_id='update_variable_23',
            append=False,
            name='{{ result("declare_variable_21").name }}',
            value=[
                {
                    "employeeTypeGroup": {
                        "uri": "{{ dag_run.conf.employeetypeuri }}",
                        "parent": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_24 = rail.SetVariableOperator(
            task_id='declare_variable_24',
            append=False,
            name='costcenterschedule',
            value=None
        )

        if_request_reporting_entity_name_present_25 = rail.IfOperator(
            task_id='if_request_reporting_entity_name_present_25',
            test='''{{ dag_run.conf.reporting_entity_name | is_truthy }}''',
            yes_task="update_variable_26",
            no_task="declare_variable_27",
        )

        update_variable_26 = rail.SetVariableOperator(
            task_id='update_variable_26',
            append=False,
            name='{{ result("declare_variable_24").name }}',
            value=[
                {
                    "costCenter": {
                        "uri": "{{ dag_run.conf.reporting_entity_name }}",
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_27 = rail.SetVariableOperator(
            task_id='declare_variable_27',
            append=False,
            name='locationSchedule',
            value=None
        )

        if_request_locationuri_present_28 = rail.IfOperator(
            task_id='if_request_locationuri_present_28',
            test='''{{ dag_run.conf.locationuri | is_truthy }}''',
            yes_task="update_variable_29",
            no_task="declare_variable_30",
        )

        update_variable_29 = rail.SetVariableOperator(
            task_id='update_variable_29',
            append=False,
            name='{{ result("declare_variable_27").name }}',
            value=[
                {
                    "location": {
                        "uri": "{{ dag_run.conf.locationuri }}",
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_30 = rail.SetVariableOperator(
            task_id='declare_variable_30',
            append=False,
            name='divisionSchedule',
            value=None
        )

        if_request_legal_entity_name_present_31 = rail.IfOperator(
            task_id='if_request_legal_entity_name_present_31',
            test='''{{ dag_run.conf.legal_entity_name | is_truthy }}''',
            yes_task="update_variable_32",
            no_task="declare_variable_36",
        )

        update_variable_32 = rail.SetVariableOperator(
            task_id='update_variable_32',
            append=False,
            name='{{ result("declare_variable_30").name }}',
            value=[
                {
                    "division": {
                        "uri": "{{ dag_run.conf.legal_entity_name }}",
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        declare_variable_36 = rail.SetVariableOperator(
            task_id='declare_variable_36',
            append=False,
            name='timezone',
            value=None
        )

        if_request_timezoneuri_present_37 = rail.IfOperator(
            task_id='if_request_timezoneuri_present_37',
            test='''{{ dag_run.conf.timezoneuri | is_truthy }}''',
            yes_task="update_variable_38",
            no_task="declare_list_39",
        )

        update_variable_38 = rail.SetVariableOperator(
            task_id='update_variable_38',
            append=False,
            name='{{ result("declare_variable_36").name }}',
            value={
                "uri": "{{ dag_run.conf.timezoneuri }}",
                "IANAName": null
            }
        )

        declare_list_39 = rail.SetVariableOperator(
            task_id='declare_list_39',
            append=False,
            name='customFieldValues',
            value=[]
        )

        if_request_location_node_present_40 = rail.IfOperator(
            task_id='if_request_location_node_present_40',
            test='''{{ dag_run.conf.location_node | is_truthy }}''',
            yes_task="insert_to_list_41",
            no_task="if_request_week_hours_present_42",
        )

        insert_to_list_41 = rail.SetVariableOperator(
            task_id='insert_to_list_41',
            append=True,
            name='{{ result("declare_list_39").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.nodeudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": "{{ dag_run.conf.location_node }}",
                "date": null,
                "dropDownOption": null,
                "number": null
            }
        )

        if_request_week_hours_present_42 = rail.IfOperator(
            task_id='if_request_week_hours_present_42',
            test='''{{ dag_run.conf.week_hours | is_truthy }}''',
            yes_task="insert_to_list_43",
            no_task="declare_list_45",
        )

        insert_to_list_43 = rail.SetVariableOperator(
            task_id='insert_to_list_43',
            append=True,
            name='{{ result("declare_list_39").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.weeklyhoursudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "text": null,
                "date": null,
                "dropDownOption": null,
                "number": "{{ dag_run.conf.week_hours }}"
            }
        )

        declare_list_45 = rail.SetVariableOperator(
            task_id='declare_list_45',
            append=False,
            name='policySets',
            value=[]
        )

        if_request_timesheettemplate_present_46 = rail.IfOperator(
            task_id='if_request_timesheettemplate_present_46',
            test='''{{ dag_run.conf.timesheettemplate | is_truthy }}''',
            yes_task="insert_to_list_47",
            no_task="log_policy_settoassign_51",
        )

        insert_to_list_47 = rail.SetVariableOperator(
            task_id='insert_to_list_47',
            append=True,
            name='{{ result("declare_list_45").name }}',
            value={
                "uri": "{{ dag_run.conf.timesheettemplate }}",
                "name": null
            }
        )

        log_policy_settoassign_51 = rail.PythonOperator(
            task_id='log_policy_settoassign_51',
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('declare_list_45')['name'])
        )

        declare_variable_52 = rail.SetVariableOperator(
            task_id='declare_variable_52',
            append=False,
            name='permission',
            value=None
        )

        declare_variable_53 = rail.SetVariableOperator(
            task_id='declare_variable_53',
            append=False,
            name='timesheetapprovalpath',
            value=None
        )

        declare_variable_54 = rail.SetVariableOperator(
            task_id='declare_variable_54',
            append=False,
            name='timesheetperiod',
            value=None
        )

        if_request_user_type_equals_to_emp_55 = rail.IfOperator(
            task_id='if_request_user_type_equals_to_emp_55',
            test='''{{ dag_run.conf.user_type == 'EMP' }}''',
            yes_task="update_variable_56",
            no_task="if_request_user_type_equals_to_mgr_59",
        )

        update_variable_56 = rail.SetVariableOperator(
            task_id='update_variable_56',
            append=False,
            name='{{ result("declare_variable_52").name }}',
            value=[
                {
                    "uri": null,
                    "name": "Project Resource"
                }
            ]
        )

        update_variable_57 = rail.SetVariableOperator(
            task_id='update_variable_57',
            append=False,
            name='{{ result("declare_variable_53").name }}',
            value={
                "uri": null,
                "name": "Supervisor"
            }
        )

        update_variable_58 = rail.SetVariableOperator(
            task_id='update_variable_58',
            append=False,
            name='{{ result("declare_variable_54").name }}',
            value=[
                {
                    "timesheetPeriod": {
                        "uri": "{{ dag_run.conf.timesheetperioduri }}",
                        "name": null
                    },
                    "effectiveDate": null
                }
            ]
        )

        if_request_user_type_equals_to_mgr_59 = rail.IfOperator(
            task_id='if_request_user_type_equals_to_mgr_59',
            test='''{{ dag_run.conf.user_type == 'MGR' }}''',
            yes_task="update_variable_60",
            no_task="create_user_62",
        )

        update_variable_60 = rail.SetVariableOperator(
            task_id='update_variable_60',
            append=False,
            name='{{ result("declare_variable_52").name }}',
            value=[
                {
                    "uri": "{{ dag_run.conf.supervisorpermissionuri }}",
                    "name": null,
                }
            ]
        )

        create_user_62 = rail.RepliconServiceOperator(
            task_id='create_user_62',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['work_email'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": dag_run.conf['work_email'],
                    "employeeId": dag_run.conf['employeeid'],
                    "department": null,
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": dag_run.conf['work_schedule'],
                                "officeSchedule": {
                                    "officeScheduleUri": null,
                                    "name": dag_run.conf['work_schedule']
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "workWeekStartDayUri": "urn:replicon:day-of-week:sunday",
                    "employmentDateRange": {
                        "startDate": {
                            "year": pendulum.now(config.pacific_timezone).year,
                            "month": pendulum.now(config.pacific_timezone).month,
                            "day": pendulum.now(config.pacific_timezone).day
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
                        "loginName": dag_run.conf['work_email'],
                        "SSOName": dag_run.conf['work_email'],
                        "password": null
                    },
                    "holidayCalendar": rail.get_dag_run_var(
                        rail.result('declare_variable_11')['name']),
                    "timeOffPolicy": null,
                    "permissionSets": rail.get_dag_run_var(
                        rail.result('declare_variable_52')['name']),
                    "policySets": rail.result('log_policy_settoassign_51'),
                    "employeeType": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "timesheetPeriodTypeUri": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": rail.get_dag_run_var(
                        rail.result('declare_variable_53')['name']),
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": rail.get_dag_run_var(
                        rail.result('declare_list_39')['name']),
                    "assignedActivities": null,
                    "timeZone": rail.get_dag_run_var(
                        rail.result('declare_variable_36')['name']),
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": rail.get_dag_run_var(
                        rail.result('declare_variable_27')['name']),
                    "divisionSchedule": rail.get_dag_run_var(
                        rail.result('declare_variable_30')['name']),
                    "costCenterSchedule": rail.get_dag_run_var(
                        rail.result('declare_variable_24')['name']),
                    "serviceCenterSchedule": null,
                    "departmentGroupSchedule": rail.get_dag_run_var(
                        rail.result('declare_variable_18')['name']),
                    "employeeTypeGroupSchedule": rail.get_dag_run_var(
                        rail.result('declare_variable_21')['name']),
                    "timesheetPeriodSchedule": rail.get_dag_run_var(
                        rail.result('declare_variable_54')['name']),
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": null,
                    "displayNameParameter": null
                }
            }
        )

        if_request_actual_termination_date_present_63 = rail.IfOperator(
            task_id='if_request_actual_termination_date_present_63',
            test='''{{ dag_run.conf.actual_termination_date | is_truthy }}''',
            yes_task="date_split_enddate_64",
            no_task="if_request_line_manager_present_66",
        )

        date_split_enddate_64 = rail.EmptyOperator(
            task_id='date_split_enddate_64',
        )

        update_employment_date_rangeforenddate_updateenddatewithnewstartdate_65 = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforenddate_updateenddatewithnewstartdate_65',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_62')['uri'],
                "dateRange": {
                    "startDate": {
                        "year":  pendulum.now(config.pacific_timezone).year,
                        "month":  pendulum.now(config.pacific_timezone).month,
                        "day":  pendulum.now(config.pacific_timezone).day,
                    },
                    "endDate": {
                        "year":  datetime.strptime(dag_run.conf['actual_termination_date'], '%Y%m%d').year,
                        "month": datetime.strptime(dag_run.conf['actual_termination_date'], '%Y%m%d').month,
                        "day":  datetime.strptime(dag_run.conf['actual_termination_date'], '%Y%m%d').day,
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_line_manager_present_66 = rail.IfOperator(
            task_id='if_request_line_manager_present_66',
            test='''{{ dag_run.conf.line_manager | is_truthy }}''',
            yes_task="if_request_line_manager_equals_to_dataworkato_service3cd9c331requestemployeeid_67",
            no_task="insert_to_list_90",
        )

        if_request_line_manager_equals_to_dataworkato_service3cd9c331requestemployeeid_67 = rail.IfOperator(
            task_id='if_request_line_manager_equals_to_dataworkato_service3cd9c331requestemployeeid_67',
            test='''{{ dag_run.conf.line_manager == dag_run.conf.employeeid }}''',
            yes_task="insert_to_list_68",
            no_task="search_users_70",
        )

        insert_to_list_68 = rail.SetVariableOperator(
            task_id='insert_to_list_68',
            append=True,
            name='{{ result("declare_list_8").name }}',
            value={
                "log": "Supervisor not assigned as User and Supervisor's employee id are the same."
            }
        )

        def get_supervisor_info(result, employeeid):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], result))))
            existing_user = list(filter(lambda x: x['employeeid'] == employeeid, map(lambda row: {
                'username': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
                'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'loginname': row['cells'][1]['textValue'],
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))

            return existing_user[0] if existing_user else {}

        search_users_70 = rail.RepliconServicePageOperator(
            task_id="search_users_70",
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
                            'text': dag_run.conf['line_manager']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result, dag_run: get_supervisor_info(
                result, dag_run.conf['line_manager'])
        )

        if_login_name_uri_present_71 = rail.IfOperator(
            task_id='if_login_name_uri_present_71',
            test='''{{ result('search_users_70') | is_truthy }}''',
            yes_task="if_split_lengthnil_greater_than_1_72",
            no_task="ice_supervisor_check_add_entry_88",
        )

        if_split_lengthnil_greater_than_1_72 = rail.IfOperator(
            task_id='if_split_lengthnil_greater_than_1_72',
            test='''{{ result('search_users_70') | is_truthy }}''',
            yes_task="log_supervisorcheck_75",
            no_task="insert_to_list_73",
        )

        insert_to_list_73 = rail.SetVariableOperator(
            task_id='insert_to_list_73',
            append=True,
            name='{{ result("declare_list_8").name }}',
            value={
                "log": "Supervisor not assigned as there are multiple users with the ID '{{ dag_run.conf.line_manager }}' in Replicon."
            }
        )

        log_supervisorcheck_75 = rail.PythonOperator(
            task_id='log_supervisorcheck_75',
            python_callable=lambda: rail.result('search_users_70')[
                'useruri'] if rail.result('search_users_70') else None
        )

        if_log_supervisorcheck_75_present_76 = rail.IfOperator(
            task_id='if_log_supervisorcheck_75_present_76',
            test='''{{ result('log_supervisorcheck_75') | is_truthy }}''',
            yes_task="get_userdataforsupervisor_77",
            no_task="ice_supervisor_check_add_entry_86",
        )

        get_userdataforsupervisor_77 = rail.RepliconServiceOperator(
            task_id='get_userdataforsupervisor_77',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('log_supervisorcheck_75') }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else None
        )

        if_userdetails_isenabled_is_true_78 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_78',
            test='''{{ result('get_userdataforsupervisor_77').userDetails.isEnabled == true }}''',
            yes_task="log_checkifsupervisorpermissionisassigned_79",
            no_task="insert_to_list_84",
        )

        log_checkifsupervisorpermissionisassigned_79 = rail.PythonOperator(
            task_id='log_checkifsupervisorpermissionisassigned_79',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_userdataforsupervisor_77')['permissionSets'], 'name', "Supervisor", 'uri')
        )

        if_log_checkifsupervisorpermissionisassigned_79_blank_80 = rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionisassigned_79_blank_80',
            test='''{{ result('log_checkifsupervisorpermissionisassigned_79') | is_falsy }}''',
            yes_task="assign_supervsior_permission_set_to_user_manager_81",
            no_task="assigninitialsupervisor_82",
        )

        assign_supervsior_permission_set_to_user_manager_81 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_manager_81',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('get_userdataforsupervisor_77').userDetails.uri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        assigninitialsupervisor_82 = rail.RepliconServiceOperator(
            task_id='assigninitialsupervisor_82',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_62').uri }}",
                "supervisorUri": "{{ result('get_userdataforsupervisor_77').userDetails.uri }}",
                "dateRange": null
            }
        )

        insert_to_list_84 = rail.SetVariableOperator(
            task_id='insert_to_list_84',
            append=True,
            name='{{ result("declare_list_8").name }}',
            value={
                "log": "Supervisor not assigned as the the Initial Supervisor '{{ dag_run.conf.line_manager }}' is in disabled status."
            }
        )

        ice_supervisor_check_add_entry_86 = rail.WriteLogOperator(
            task_id='ice_supervisor_check_add_entry_86',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="Add",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "userloginname": "{{ dag_run.conf.work_email }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorempid": "{{ dag_run.conf.line_manager }}",
                "useruri": "{{ result('create_user_62').uri }}",
                "action": "Add",
                "status": "",
                "childjobid": "{{ dag_run_ecid() }}",
                "effectivedate": "{{ current_time('%m_%d_%Y') }}"
            }
        )

        ice_supervisor_check_add_entry_88 = rail.WriteLogOperator(
            task_id='ice_supervisor_check_add_entry_88',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="Add",
            severity="Info",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "userloginname": "{{ dag_run.conf.work_email }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorempid": "{{ dag_run.conf.line_manager }}",
                "useruri": "{{ result('create_user_62').uri }}",
                "action": "Add",
                "status": "",
                "childjobid": "{{ dag_run_ecid() }}",
                "effectivedate": "{{ current_time('%m_%d_%Y') }}"
            }
        )

        insert_to_list_90 = rail.SetVariableOperator(
            task_id='insert_to_list_90',
            append=True,
            name='{{ result("declare_list_8").name }}',
            value={
                "log": "User created - Supervisor not assigned as the the Initial Supervisor was not present in the input file."
            }
        )

        def combine_logs(list_name):
            logs = [loginfo['log'] for loginfo in rail.get_dag_run_var(
                rail.result(list_name)['name'])]
            return rail.smartjoin_by_delim(logs, '|') if logs else None

        log_exceptions_91 = rail.PythonOperator(
            task_id='log_exceptions_91',
            python_callable=lambda: combine_logs('declare_list_8')
        )

        intercontinentalexchange_user_import_logs_add_entry_92 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_user_import_logs_add_entry_92',
            message=lambda: rail.result('log_exceptions_91') if rail.result(
                'log_exceptions_91') else "Success",
            severity=lambda: "Exception" if rail.result(
                'log_exceptions_91') else "Success",
            properties=lambda dag_run: {
                "Empid": dag_run.conf['employeeid'],
                "Username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "Action": "Add",
                "Status": "Exception" if rail.result('log_exceptions_91') else "Success",
                "Details": "User created partially - " + rail.result('log_exceptions_91') if rail.result(
                    'log_exceptions_91') else "User created successfully",
                "Jobid": get_dagrun_ecid(dag_run)
            }
        )

        intercontinentalexchange_user_import_logs_add_entry_94 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_user_import_logs_add_entry_94',
            message="{{ get_error_message() }}",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "Empid": "{{ dag_run.conf.employeeid }}",
                "Username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "Action": "Add",
                "Status": "Error",
                "Details": "{{ get_error_message() }}",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> search_users_4 >> if_login_name_textvalue_present_5
        if_login_name_textvalue_present_5 >> rail.Label(
            'Yes') >> intercontinentalexchange_user_import_logs_add_entry_6 >> log_to_sumo
        if_login_name_textvalue_present_5 >> rail.Label(
            'No') >> declare_list_8 >> declare_variable_10 >> declare_variable_11 >> if_request_holidaycalendaruri_present_12
        if_request_holidaycalendaruri_present_12 >> rail.Label(
            'Yes') >> update_variable_13 >> get_all_permission_sets_16
        if_request_holidaycalendaruri_present_12 >> rail.Label(
            'No') >> insert_to_list_15 >> get_all_permission_sets_16 >> declare_list_17 >> declare_variable_18 >> \
            if_request_department_present_19
        if_request_department_present_19 >> rail.Label(
            'Yes') >> update_variable_20 >> declare_variable_21
        if_request_department_present_19 >> rail.Label(
            'No') >> declare_variable_21 >> if_request_employeetypeuri_present_22
        if_request_employeetypeuri_present_22 >> rail.Label(
            'Yes') >> update_variable_23 >> declare_variable_24
        if_request_employeetypeuri_present_22 >> rail.Label(
            'No') >> declare_variable_24 >> if_request_reporting_entity_name_present_25
        if_request_reporting_entity_name_present_25 >> rail.Label(
            'Yes') >> update_variable_26 >> declare_variable_27
        if_request_reporting_entity_name_present_25 >> rail.Label(
            'No') >> declare_variable_27 >> if_request_locationuri_present_28
        if_request_locationuri_present_28 >> rail.Label(
            'Yes') >> update_variable_29 >> declare_variable_30
        if_request_locationuri_present_28 >> rail.Label(
            'No') >> declare_variable_30 >> if_request_legal_entity_name_present_31
        if_request_legal_entity_name_present_31 >> rail.Label(
            'Yes') >> update_variable_32 >> declare_variable_36
        if_request_legal_entity_name_present_31 >> rail.Label(
            'No') >> declare_variable_36 >> if_request_timezoneuri_present_37
        if_request_timezoneuri_present_37 >> rail.Label(
            'Yes') >> update_variable_38 >> declare_list_39
        if_request_timezoneuri_present_37 >> rail.Label(
            'No') >> declare_list_39 >> if_request_location_node_present_40
        if_request_location_node_present_40 >> rail.Label(
            'Yes') >> insert_to_list_41 >> if_request_week_hours_present_42
        if_request_location_node_present_40 >> rail.Label(
            'No') >> if_request_week_hours_present_42
        if_request_week_hours_present_42 >> rail.Label(
            'Yes') >> insert_to_list_43 >> declare_list_45
        if_request_week_hours_present_42 >> rail.Label(
            'No') >> declare_list_45 >> if_request_timesheettemplate_present_46
        if_request_timesheettemplate_present_46 >> rail.Label(
            'Yes') >> insert_to_list_47 >> log_policy_settoassign_51
        if_request_timesheettemplate_present_46 >> rail.Label(
            'No') >> log_policy_settoassign_51 >> declare_variable_52 >> declare_variable_53 >> declare_variable_54 >> \
            if_request_user_type_equals_to_emp_55
        if_request_user_type_equals_to_emp_55 >> rail.Label(
            'Yes') >> update_variable_56 >> update_variable_57 >> update_variable_58 >> if_request_user_type_equals_to_mgr_59
        if_request_user_type_equals_to_emp_55 >> rail.Label(
            'No') >> if_request_user_type_equals_to_mgr_59
        if_request_user_type_equals_to_mgr_59 >> rail.Label(
            'Yes') >> update_variable_60 >> create_user_62
        if_request_user_type_equals_to_mgr_59 >> rail.Label(
            'No') >> create_user_62 >> if_request_actual_termination_date_present_63
        if_request_actual_termination_date_present_63 >> rail.Label(
            'Yes') >> date_split_enddate_64 >> update_employment_date_rangeforenddate_updateenddatewithnewstartdate_65 >> \
            if_request_line_manager_present_66
        if_request_actual_termination_date_present_63 >> rail.Label(
            'No') >> if_request_line_manager_present_66
        if_request_line_manager_present_66 >> rail.Label(
            'Yes') >> if_request_line_manager_equals_to_dataworkato_service3cd9c331requestemployeeid_67
        if_request_line_manager_equals_to_dataworkato_service3cd9c331requestemployeeid_67 >> rail.Label(
            'Yes') >> insert_to_list_68 >> log_exceptions_91
        if_request_line_manager_equals_to_dataworkato_service3cd9c331requestemployeeid_67 >> rail.Label(
            'No') >> search_users_70 >> if_login_name_uri_present_71
        if_login_name_uri_present_71 >> rail.Label(
            'Yes') >> if_split_lengthnil_greater_than_1_72
        if_split_lengthnil_greater_than_1_72 >> rail.Label(
            'No') >> insert_to_list_73 >> log_exceptions_91
        if_split_lengthnil_greater_than_1_72 >> rail.Label(
            'Yes') >> log_supervisorcheck_75 >> if_log_supervisorcheck_75_present_76
        if_log_supervisorcheck_75_present_76 >> rail.Label(
            'Yes') >> get_userdataforsupervisor_77 >> if_userdetails_isenabled_is_true_78
        if_userdetails_isenabled_is_true_78 >> rail.Label(
            'Yes') >> log_checkifsupervisorpermissionisassigned_79 >> if_log_checkifsupervisorpermissionisassigned_79_blank_80
        if_userdetails_isenabled_is_true_78 >> rail.Label(
            'No') >> insert_to_list_84 >> log_exceptions_91
        if_log_checkifsupervisorpermissionisassigned_79_blank_80 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_manager_81 >> assigninitialsupervisor_82
        if_log_checkifsupervisorpermissionisassigned_79_blank_80 >> rail.Label(
            'No') >> assigninitialsupervisor_82 >> log_exceptions_91
        if_log_supervisorcheck_75_present_76 >> rail.Label(
            'No') >> ice_supervisor_check_add_entry_86 >> log_exceptions_91
        if_login_name_uri_present_71 >> rail.Label(
            'No') >> ice_supervisor_check_add_entry_88 >> log_exceptions_91
        if_request_line_manager_present_66 >> rail.Label(
            'No') >> insert_to_list_90 >> log_exceptions_91 >> intercontinentalexchange_user_import_logs_add_entry_92 >> \
            intercontinentalexchange_user_import_logs_add_entry_94 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)

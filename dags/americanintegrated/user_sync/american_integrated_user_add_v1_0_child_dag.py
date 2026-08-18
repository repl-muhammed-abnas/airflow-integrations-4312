
from datetime import timedelta, datetime
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
from americanintegrated.user_sync.mapper.default_users_mapper import american_integration_default_users_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.user_import_add_child,
        description=f'American Integrated User Add V1.0_child {config.instance}',
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
            no_task='declare_list_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_2',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_2 = rail.SetVariableOperator(
            task_id='declare_list_2',
            append=False,
            name='exception logger',
            value=[]
        )

        def required_field_validation(dag_run):
            validation_message = []
            if not dag_run.conf.get('firstname'):
                validation_message.append('Employee First  Name not present')
            if not dag_run.conf.get('lastname'):
                validation_message.append('Employee Last  Name not present')
            if not dag_run.conf.get('hiredate'):
                validation_message.append('Hire date not present')
            if not dag_run.conf.get('payfrequency'):
                validation_message.append(
                    'Pay Frequency considered for Employee type not present')
            if not dag_run.conf.get('position'):
                validation_message.append('department code not present')
            return rail.smartjoin_by_delim(validation_message, ';')

        log_checkifrequiredfieldsarenotthere_4 = rail.PythonOperator(
            task_id='log_checkifrequiredfieldsarenotthere_4',
            python_callable=required_field_validation
        )

        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 = rail.IfOperator(
            task_id='if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5',
            test='''{{ result('log_checkifrequiredfieldsarenotthere_4') | is_truthy }}''',
            yes_task="american_integrated_user_import_logs_add_entry_6_6_6",
            no_task="if_request_status_contains_inactive_8",
        )

        american_integrated_user_import_logs_add_entry_6_6_6 = rail.WriteLogOperator(
            task_id='american_integrated_user_import_logs_add_entry_6_6_6',
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "Employeeid": dag_run.conf['employeenumber'],
                "Username": dag_run.conf['email'] if dag_run.conf['email'] else f"{dag_run.conf['firstname']} {dag_run.conf['lastname']}",
                "Status": "Skipped",
                "Details": rail.result('log_checkifrequiredfieldsarenotthere_4'),
                "action": "Add"
            }
        )

        if_request_status_contains_inactive_8 = rail.IfOperator(
            task_id='if_request_status_contains_inactive_8',
            test='''{{ dag_run.conf.status | matches('Inactive') }}''',
            yes_task="american_integrated_user_import_logs_add_entry_6_6_9",
            no_task="american_integration_default_mapper_users_search_entries_11",
        )

        american_integrated_user_import_logs_add_entry_6_6_9 = rail.WriteLogOperator(
            task_id='american_integrated_user_import_logs_add_entry_6_6_9',
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "Employeeid": dag_run.conf['employeenumber'],
                "Username": dag_run.conf['email'] if dag_run.conf['email'] else f"{dag_run.conf['firstname']} {dag_run.conf['lastname']}",
                "Status": "Skipped",
                "Details": "User not added since the status is received as Inactive",
                "action": "Add"
            }
        )

        american_integration_default_mapper_users_search_entries_11 = rail.PythonOperator(
            task_id='american_integration_default_mapper_users_search_entries_11',
            python_callable=lambda:  next(iter(filter(
                lambda x: x["default"] == "default", american_integration_default_users_mapper)))
        )

        _adhoc_http_action_12 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_12',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
        )

        def get_employee_type(dag_run):
            employee_type = None
            if dag_run.conf['payfrequency'].lower() == 'bi-weekly':
                employee_type = "Salaried Employee"
            elif dag_run.conf['payfrequency'].lower() == 'weekly':
                employee_type = "Hourly Employee"
            return employee_type

        log_employeetypenamederived_13 = rail.PythonOperator(
            task_id='log_employeetypenamederived_13',
            python_callable=get_employee_type
        )

        log_required_employee_type_uri_14 = rail.PythonOperator(
            task_id='log_required_employee_type_uri_14',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('_adhoc_http_action_12'), 'displayText', rail.result(
                'log_employeetypenamederived_13'), 'uri') if rail.result('_adhoc_http_action_12') else None
        )

        if_log_required_employee_type_uri_14_blank_15 = rail.IfOperator(
            task_id='if_log_required_employee_type_uri_14_blank_15',
            test='''{{ result('log_required_employee_type_uri_14') | is_falsy }}''',
            yes_task="american_integrated_user_import_logs_add_entry_6_6_16",
            no_task="get_all_departmentswithcode_18",
        )

        american_integrated_user_import_logs_add_entry_6_6_16 = rail.WriteLogOperator(
            task_id='american_integrated_user_import_logs_add_entry_6_6_16',
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "Employeeid": dag_run.conf['employeenumber'],
                "Username": dag_run.conf['email'] if dag_run.conf['email'] else f"{dag_run.conf['firstname']} {dag_run.conf['lastname']}",
                "Status": "Skipped",
                "Details": "Employee Type not present in Replicon",
                "action": "Add"
            }
        )

        get_all_departmentswithcode_18 = rail.RepliconServiceOperator(
            task_id='get_all_departmentswithcode_18',
            endpoint='/services/DepartmentListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:department-list-column:department",
                    "urn:replicon:department-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(map(lambda row: {
                "name": row['cells'][0]['textValue'],
                "uri": row['cells'][0]['uri'],
                "code": row['cells'][1].get('textValue') if row['cells'][1].get('textValue') else ""
            }, data['rows']))
        )

        log_required_department_uri_22 = rail.PythonOperator(
            task_id='log_required_department_uri_22',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_departmentswithcode_18'), 'code', dag_run.conf['position'], 'uri') if rail.result('get_all_departmentswithcode_18') else None
        )

        if_log_required_department_uri_22_blank_23 = rail.IfOperator(
            task_id='if_log_required_department_uri_22_blank_23',
            test='''{{ result('log_required_department_uri_22') | is_falsy }}''',
            yes_task="american_integrated_user_import_logs_add_entry_6_6_24",
            no_task="log_time_zonefrommapper_26",
        )

        american_integrated_user_import_logs_add_entry_6_6_24 = rail.WriteLogOperator(
            task_id='american_integrated_user_import_logs_add_entry_6_6_24',
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "Employeeid": dag_run.conf['employeenumber'],
                "Username": dag_run.conf['email'] if dag_run.conf['email'] else f"{dag_run.conf['firstname']} {dag_run.conf['lastname']}",
                "Status": "Skipped",
                "Details": "Department not present in Replicon",
                "action": "Add"
            }
        )

        def get_default_from_mapper(entity_type):
            entity_types = list(filter(
                lambda x: x['default'] == "default"
                and x['type'] == entity_type, american_integration_default_users_mapper))
            return entity_types[0]['defaulturi'] if entity_types else None

        def get_value_from_mapper(entity_type):
            entity_types = list(filter(
                lambda x: x['default'] == "default"
                and x['type'] == entity_type, american_integration_default_users_mapper))
            return entity_types[0]['value'] if entity_types else None

        log_time_zonefrommapper_26 = rail.PythonOperator(
            task_id='log_time_zonefrommapper_26',
            python_callable=lambda:  get_default_from_mapper('timezone')
        )

        log_required_time_off_template_name_27 = rail.PythonOperator(
            task_id='log_required_time_off_template_name_27',
            python_callable=lambda: get_value_from_mapper("time off template")
        )

        def get_value_mapper_info(dag_run, type_name):
            entity_types = list(filter(
                lambda x: x['default'] == "default"
                and x['type'] == type_name
                and x['defaulturi'].lower() == dag_run.conf['payfrequency'].lower(), american_integration_default_users_mapper))
            return entity_types[0]['value'] if entity_types else None

        log_required_timesheet_template_name_28 = rail.PythonOperator(
            task_id='log_required_timesheet_template_name_28',
            python_callable=lambda dag_run: get_value_mapper_info(
                dag_run, "timesheettemplate")
        )

        declare_variable_29 = rail.SetVariableOperator(
            task_id='declare_variable_29',
            append=False,
            name='payrule',
            value=None
        )

        log_requiredpayrule_name_30 = rail.PythonOperator(
            task_id='log_requiredpayrule_name_30',
            python_callable=lambda dag_run: get_value_mapper_info(
                dag_run, "payrule")
        )

        if_log_requiredpayrule_name_30_present_31 = rail.IfOperator(
            task_id='if_log_requiredpayrule_name_30_present_31',
            test='''{{ result('log_requiredpayrule_name_30') | is_truthy }}''',
            yes_task="update_variable_32",
            no_task="log_required_time_off_approval_path_name_33",
        )

        update_variable_32 = rail.SetVariableOperator(
            task_id='update_variable_32',
            append=False,
            name='{{ result("declare_variable_29").name }}',
            value=[
                {
                    "payRuleScript": {
                        "uri": null,
                        "name": "{{ result('log_requiredpayrule_name_30') }}"
                    },
                    "effectiveDate": null
                }
            ]
        )

        log_required_time_off_approval_path_name_33 = rail.PythonOperator(
            task_id='log_required_time_off_approval_path_name_33',
            python_callable=lambda:  get_value_from_mapper(
                "time off approval path")
        )

        log_required_timesheet_approval_path_name_34 = rail.PythonOperator(
            task_id='log_required_timesheet_approval_path_name_34',
            python_callable=lambda: get_value_from_mapper(
                "timesheet approval path")
        )

        log_required_holiday_calendar_name_35 = rail.PythonOperator(
            task_id='log_required_holiday_calendar_name_35',
            python_callable=lambda:  get_value_from_mapper("holiday calendar")
        )

        log_required_workweek_name_36 = rail.PythonOperator(
            task_id='log_required_workweek_name_36',
            python_callable=lambda:  get_default_from_mapper("workweek")
        )

        log_required_authentication_type_37 = rail.PythonOperator(
            task_id='log_required_authentication_type_37',
            python_callable=lambda:  "urn:replicon:user-authentication-type:replicon"
        )

        def get_permissions_from_mapper(dag_run):
            entity_types = list(filter(
                lambda x: x['default'] == "default"
                and x['type'] == "permission"
                and x['value'].lower() == dag_run.conf['position'].lower(), american_integration_default_users_mapper))
            return [permission_info['defaulturi'] for permission_info in entity_types] if entity_types else []

        log_required_user_permission_set_38 = rail.PythonOperator(
            task_id='log_required_user_permission_set_38',
            python_callable=get_permissions_from_mapper
        )

        def get_permission_uri():
            mapper_permissions = []
            for permission_name in rail.result('log_required_user_permission_set_38'):
                mapper_permissions.append(
                    {
                        'name': permission_name,
                        'uri': None
                    })
            return mapper_permissions

        parse_json_39 = rail.PythonOperator(
            task_id='parse_json_39',
            python_callable=get_permission_uri
        )

        invoke_custom_ruby_code_44 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_44',
            python_callable=lambda: '''{
                "date": _('dag_run.conf.hiredate.to_date"
            }'''
        )

        log_loginnamederived_45 = rail.PythonOperator(
            task_id='log_loginnamederived_45',
            python_callable=lambda dag_run:  dag_run.conf['email'] if dag_run.conf[
                'email'] else f"{dag_run.conf['firstname']} {dag_run.conf['lastname']}"
        )

        declare_variable_46 = rail.SetVariableOperator(
            task_id='declare_variable_46',
            append=False,
            name='email',
            value=None
        )

        if_request_email_present_47 = rail.IfOperator(
            task_id='if_request_email_present_47',
            test='''{{ dag_run.conf.email | is_truthy  and dag_run.conf.email | matches('@') }}''',
            yes_task="update_variable_48",
            no_task="create_user_49",
        )

        update_variable_48 = rail.SetVariableOperator(
            task_id='update_variable_48',
            append=False,
            name='{{ result("declare_variable_46").name }}',
            value="{{ dag_run.conf.email }}"
        )

        create_user_49 = rail.RepliconServiceOperator(
            task_id='create_user_49',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": rail.result('log_loginnamederived_45'),
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": rail.get_dag_run_var(rail.result('declare_variable_46')['name']),
                    "employeeId": dag_run.conf['employeenumber'],
                    "department": {
                        "uri": rail.result('log_required_department_uri_22'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": rail.result('log_required_workweek_name_36'),
                    "employmentDateRange": {
                        "startDate": {
                            "year": datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d').year,
                            "month": datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d').month,
                            "day": datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d').day
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            rail.result('log_required_authentication_type_37')
                        ],
                        "isLoginEnabled": "true",
                        "loginName": rail.result('log_loginnamederived_45'),
                        "SSOName": null,
                        "password": "Replicon@12#"
                    },
                    "holidayCalendar": {
                        "uri": null,
                        "name": rail.result('log_required_holiday_calendar_name_35')
                    },
                    "timeOffPolicy": null,
                    "permissionSets": rail.result('parse_json_39'),
                    "policySets": [
                        {
                            "uri": null,
                            "name": rail.result('log_required_time_off_template_name_27')
                        },
                        {
                            "uri": null,
                            "name": rail.result('log_required_timesheet_template_name_28')
                        }
                    ],
                    "employeeType": {
                        "uri": rail.result('log_required_employee_type_uri_14'),
                        "name": null
                    },
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {
                        "uri": null,
                        "name": rail.result('log_required_timesheet_approval_path_name_34')
                    },
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": {
                        "uri": null,
                        "name": rail.result('log_required_time_off_approval_path_name_33')
                    },
                    "customFieldValues": [],
                    "assignedActivities": [],
                    "timeZone": {
                        "uri": rail.result('log_time_zonefrommapper_26'),
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
                    "payRuleScriptSchedule": rail.get_dag_run_var(rail.result('declare_variable_29')['name'])
                }
            }
        )

        log_defaulttimeofftypetoassign_50 = rail.PythonOperator(
            task_id='log_defaulttimeofftypetoassign_50',
            python_callable=lambda:  get_value_from_mapper("timeofftype")
        )

        get_all_time_off_types_51 = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_51',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data=None
        )

        log_time_offtypeurifor_holiday_52 = rail.PythonOperator(
            task_id='log_time_offtypeurifor_holiday_52',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types_51'), "displayText", rail.result(
                'log_defaulttimeofftypetoassign_50'), 'uri') if rail.result('get_all_time_off_types_51') else None
        )

        if_log_time_offtypeurifor_holiday_52_present_53 = rail.IfOperator(
            task_id='if_log_time_offtypeurifor_holiday_52_present_53',
            test='''{{ result('log_time_offtypeurifor_holiday_52') | is_truthy }}''',
            yes_task="assign_holiday_time_offtype_54",
            no_task="remove_timeoff_assignments_56",
        )

        assign_holiday_time_offtype_54 = rail.RepliconServiceOperator(
            task_id='assign_holiday_time_offtype_54',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_49').uri }}",
                "timeOffTypeUris": ["{{ result('log_time_offtypeurifor_holiday_52') }}"]
            }
        )

        remove_timeoff_assignments_56 = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignments_56',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_49').uri }}",
                "timeOffTypeUris": []
            }
        )

        _adhoc_http_action_57 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_57',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        log_office_schedulename_58 = rail.PythonOperator(
            task_id='log_office_schedulename_58',
            python_callable=lambda:  get_value_from_mapper("schedule")
        )

        _adhoc_http_action_59 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_59',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        log_gettherequiredofficeschedule_uri_60 = rail.PythonOperator(
            task_id='log_gettherequiredofficeschedule_uri_60',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('_adhoc_http_action_59'), "displayText", rail.result(
                'log_office_schedulename_58'), 'uri') if rail.result('_adhoc_http_action_59') else None
        )

        declare_variable_system_default_61 = rail.SetVariableOperator(
            task_id='declare_variable_system_default_61',
            append=False,
            name='Timesheet Period uri',
            value=None
        )

        if_request_payfrequency_present_62 = rail.IfOperator(
            task_id='if_request_payfrequency_present_62',
            test='''{{ dag_run.conf.payfrequency | is_truthy and dag_run.conf.payfrequency | lower =='bi-weekly' }}''',
            yes_task="update_variable_basedon_employeetype_63",
            no_task="update_timesheet_period_type_for_user_64",
        )

        update_variable_basedon_employeetype_63 = rail.SetVariableOperator(
            task_id='update_variable_basedon_employeetype_63',
            append=False,
            name='{{ result("declare_variable_system_default_61").name }}',
            value='urn:replicon:timesheet-period-type:based-on-employee-type-assignment'
        )

        update_timesheet_period_type_for_user_64 = rail.RepliconServiceOperator(
            task_id='update_timesheet_period_type_for_user_64',
            endpoint="/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser",
            data={
                "userUri": "{{ result('create_user_49').uri }}",
                "timesheetPeriodTypeUri": "{{ result('declare_variable_system_default_61').value }}"
            }
        )

        if_log_gettherequiredofficeschedule_uri_60_present_65 = rail.IfOperator(
            task_id='if_log_gettherequiredofficeschedule_uri_60_present_65',
            test='''{{ result('log_gettherequiredofficeschedule_uri_60') | is_truthy }}''',
            yes_task="assign_initial_schedule_66",
            no_task="american_integrated_user_import_logs_add_entry_6_6_68",
        )

        assign_initial_schedule_66 = rail.RepliconServiceOperator(
            task_id='assign_initial_schedule_66',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user_49').uri }}",
                "scheduleEntries":
                [
                    {
                        "schedulePolicy":
                        {
                            "officeSchedule":
                            {
                                "officeScheduleUri": "{{ result('log_gettherequiredofficeschedule_uri_60') }}"
                            },
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        }
                    }
                ]
            }
        )

        american_integrated_user_import_logs_add_entry_6_6_68 = rail.WriteLogOperator(
            task_id='american_integrated_user_import_logs_add_entry_6_6_68',
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "Employeeid": dag_run.conf['employeenumber'],
                "Username": dag_run.conf['email'] if dag_run.conf['email'] else f"{dag_run.conf['firstname']} {dag_run.conf['lastname']}",
                "Status": "Success",
                "Details": "The User added successfully",
                "action": "Add"
            }
        )

        catch_69_69_69 = rail.EmptyOperator(
            task_id='catch_69_69_69',
            trigger_rule='one_failed',
        )

        american_integrated_user_import_logs_add_entry_6_6_70 = rail.WriteLogOperator(
            task_id='american_integrated_user_import_logs_add_entry_6_6_70',
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "Employeeid": dag_run.conf['employeenumber'],
                "Username": dag_run.conf['email'] if dag_run.conf['email'] else f"{dag_run.conf['firstname']} {dag_run.conf['lastname']}",
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}"),
                "action": "Add"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> declare_list_2
        declare_list_2 >> log_checkifrequiredfieldsarenotthere_4 >> if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5
        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 >> rail.Label(
            'Yes') >> american_integrated_user_import_logs_add_entry_6_6_6 >> catch_69_69_69
        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 >> rail.Label(
            'No') >> if_request_status_contains_inactive_8
        if_request_status_contains_inactive_8 >> rail.Label(
            'Yes') >> american_integrated_user_import_logs_add_entry_6_6_9 >> catch_69_69_69
        american_integration_default_mapper_users_search_entries_11
        if_request_status_contains_inactive_8 >> rail.Label(
            'No') >> american_integration_default_mapper_users_search_entries_11 >> _adhoc_http_action_12 >> \
            log_employeetypenamederived_13 >> log_required_employee_type_uri_14 >> if_log_required_employee_type_uri_14_blank_15
        if_log_required_employee_type_uri_14_blank_15 >> rail.Label(
            'Yes') >> american_integrated_user_import_logs_add_entry_6_6_16 >> catch_69_69_69
        get_all_departmentswithcode_18
        if_log_required_employee_type_uri_14_blank_15 >> rail.Label(
            'No') >> get_all_departmentswithcode_18 >> log_required_department_uri_22 >> if_log_required_department_uri_22_blank_23
        if_log_required_department_uri_22_blank_23 >> rail.Label(
            'Yes') >> american_integrated_user_import_logs_add_entry_6_6_24 >> catch_69_69_69
        log_time_zonefrommapper_26
        if_log_required_department_uri_22_blank_23 >> rail.Label(
            'No') >> log_time_zonefrommapper_26 >> log_required_time_off_template_name_27 >> log_required_timesheet_template_name_28 >> \
            declare_variable_29 >> log_requiredpayrule_name_30 >> if_log_requiredpayrule_name_30_present_31
        if_log_requiredpayrule_name_30_present_31 >> rail.Label(
            'Yes') >> update_variable_32 >> log_required_time_off_approval_path_name_33
        if_log_requiredpayrule_name_30_present_31 >> rail.Label(
            'No') >> log_required_time_off_approval_path_name_33 >> log_required_timesheet_approval_path_name_34 >> \
            log_required_holiday_calendar_name_35 >> log_required_workweek_name_36 >> log_required_authentication_type_37 >> \
            log_required_user_permission_set_38 >> parse_json_39 >> invoke_custom_ruby_code_44 >> \
            log_loginnamederived_45 >> declare_variable_46 >> if_request_email_present_47
        if_request_email_present_47 >> rail.Label(
            'Yes') >> update_variable_48 >> create_user_49
        if_request_email_present_47 >> rail.Label(
            'No') >> create_user_49 >> log_defaulttimeofftypetoassign_50 >> get_all_time_off_types_51 >> \
            log_time_offtypeurifor_holiday_52 >> if_log_time_offtypeurifor_holiday_52_present_53
        if_log_time_offtypeurifor_holiday_52_present_53 >> rail.Label(
            'Yes') >> assign_holiday_time_offtype_54 >> _adhoc_http_action_57
        if_log_time_offtypeurifor_holiday_52_present_53 >> rail.Label(
            'No') >> remove_timeoff_assignments_56 >> _adhoc_http_action_57 >> log_office_schedulename_58 >> \
            _adhoc_http_action_59 >> log_gettherequiredofficeschedule_uri_60 >> \
            declare_variable_system_default_61 >> if_request_payfrequency_present_62
        if_request_payfrequency_present_62 >> rail.Label(
            'Yes') >> update_variable_basedon_employeetype_63 >> update_timesheet_period_type_for_user_64
        if_request_payfrequency_present_62 >> rail.Label(
            'No') >> update_timesheet_period_type_for_user_64 >> if_log_gettherequiredofficeschedule_uri_60_present_65
        if_log_gettherequiredofficeschedule_uri_60_present_65 >> rail.Label('No') >> \
            american_integrated_user_import_logs_add_entry_6_6_68
        if_log_gettherequiredofficeschedule_uri_60_present_65 >> rail.Label(
            'Yes') >> assign_initial_schedule_66 >> american_integrated_user_import_logs_add_entry_6_6_68 >> \
            catch_69_69_69 >> american_integrated_user_import_logs_add_entry_6_6_70 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)

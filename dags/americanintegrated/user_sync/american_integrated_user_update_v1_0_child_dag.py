
from datetime import timedelta, datetime
import pendulum
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
from americanintegrated.user_sync.mapper.default_users_mapper import american_integration_default_users_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.user_import_update_child,
        description=f'American Integrated User Update V1.0_child  {config.instance}',
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

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='job logs',
            value=[]
        )

        declare_variable_4 = rail.SetVariableOperator(
            task_id='declare_variable_4',
            append=False,
            name='department based update',
            value=None
        )

        bulk_get_users3_6 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_6',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_request_status_contains_inactive_7 = rail.IfOperator(
            task_id='if_request_status_contains_inactive_7',
            test='''{{ dag_run.conf.status | matches('Inactive')  and result('bulk_get_users3_6')[0].userDetails.isEnabled | is_truthy }}''',
            yes_task="disable_login_8",
            no_task="if_request_status_contains_inactive_11",
        )

        disable_login_8 = rail.RepliconServiceOperator(
            task_id='disable_login_8',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        american_integrated_user_import_logs_add_entry_9 = rail.WriteLogOperator(
            task_id='american_integrated_user_import_logs_add_entry_9',
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "Employeeid": dag_run.conf['employeenumber'],
                "Username": dag_run.conf['email'] if dag_run.conf['email'] else f"{dag_run.conf['firstname']} {dag_run.conf['lastname']}",
                "Status": "Success",
                "Details": "NA",
                "action": "Disable"
            }
        )

        if_request_status_contains_inactive_11 = rail.IfOperator(
            task_id='if_request_status_contains_inactive_11',
            test='''{{ dag_run.conf.status | matches('Inactive')  and result('bulk_get_users3_6')[0].userDetails.isEnabled | is_falsy }}''',
            yes_task="american_integrated_user_import_logs_add_entry_12",
            no_task="if_request_status_contains_active_14",
        )

        american_integrated_user_import_logs_add_entry_12 = rail.WriteLogOperator(
            task_id='american_integrated_user_import_logs_add_entry_12',
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "Employeeid": dag_run.conf['employeenumber'],
                "Username": dag_run.conf['email'] if dag_run.conf['email'] else f"{dag_run.conf['firstname']} {dag_run.conf['lastname']}",
                "Status": "Skipped",
                "Details": "User is already disabled in Replicon",
                "action": "Disable"
            }
        )

        if_request_status_contains_active_14 = rail.IfOperator(
            task_id='if_request_status_contains_active_14',
            test='''{{ dag_run.conf.status | matches('Active') and result('bulk_get_users3_6')[0].userDetails.isEnabled | is_falsy }}''',
            yes_task="enable_login_15",
            no_task="american_integration_default_mapper_users_search_entries_16",
        )

        enable_login_15 = rail.RepliconServiceOperator(
            task_id='enable_login_15',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        american_integration_default_mapper_users_search_entries_16 = rail.PythonOperator(
            task_id='american_integration_default_mapper_users_search_entries_16',
            python_callable=lambda:  list(filter(
                lambda x: x['default'] == "default", american_integration_default_users_mapper))
        )

        def get_timesheet_template(dag_run):
            entity_types = list(filter(
                lambda x: x['default'] == "default"
                and x['type'] == "timesheettemplate"
                and x['defaulturi'].lower() == dag_run.conf['payfrequency'].lower(), american_integration_default_users_mapper))
            return entity_types[0]['value'] if entity_types else None

        log_timesheet_templatename_17 = rail.PythonOperator(
            task_id='log_timesheet_templatename_17',
            python_callable=get_timesheet_template
        )

        if_timesheettemplate_name_not_equals_to_dataloggerlog_timesheet_templatename_17message_18 = rail.IfOperator(
            task_id='if_timesheettemplate_name_not_equals_to_dataloggerlog_timesheet_templatename_17message_18',
            test='''{{ (result('bulk_get_users3_6')[0].timesheetTemplate | is_falsy and result('log_timesheet_templatename_17') | is_truthy) or (result('bulk_get_users3_6')[0].timesheetTemplate.name != result('log_timesheet_templatename_17')) }}''',
            yes_task="get_all_policy_sets_19",
            no_task="log_employeetypenamederived_23",
        )

        get_all_policy_sets_19 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_19',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        log_timesheettemplateuri_20 = rail.PythonOperator(
            task_id='log_timesheettemplateuri_20',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets_19'), 'displayText', rail.result(
                'log_timesheet_templatename_17'), 'uri')
        )

        assign_timesheet_template_21 = rail.RepliconServiceOperator(
            task_id='assign_timesheet_template_21',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('log_timesheettemplateuri_20') }}"
            }
        )

        insert_to_list_22 = rail.SetVariableOperator(
            task_id='insert_to_list_22',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Timesheet template updated"
            }
        )

        def get_employee_type(dag_run):
            employee_type = None
            if dag_run.conf['payfrequency'].lower() == 'bi-weekly':
                employee_type = "Salaried Employee"
            elif dag_run.conf['payfrequency'].lower() == 'weekly':
                employee_type = "Hourly Employee"
            return employee_type

        log_employeetypenamederived_23 = rail.PythonOperator(
            task_id='log_employeetypenamederived_23',
            python_callable=get_employee_type
        )

        def get_payfrequency_validation(dag_run):
            emp_type = rail.result('bulk_get_users3_6')[
                0]['employeeType']['displayText']
            emp_type_lower = emp_type.lower() if emp_type else None
            if dag_run.conf['payfrequency'] and dag_run.conf['payfrequency'] != emp_type_lower:
                return True
            return False

        if_request_payfrequency_present_24 = rail.IfOperator(
            task_id='if_request_payfrequency_present_24',
            test=get_payfrequency_validation,
            yes_task="_adhoc_http_action_25",
            no_task="if_request_position_present_59",
        )

        _adhoc_http_action_25 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_25',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
        )

        log_required_employee_type_uri_26 = rail.PythonOperator(
            task_id='log_required_employee_type_uri_26',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('_adhoc_http_action_25'), 'displayText', rail.result(
                'log_employeetypenamederived_23'), 'uri') if rail.result('_adhoc_http_action_25') else None
        )

        if_log_required_employee_type_uri_26_blank_27 = rail.IfOperator(
            task_id='if_log_required_employee_type_uri_26_blank_27',
            test='''{{ result('log_required_employee_type_uri_26') | is_falsy }}''',
            yes_task="insert_to_list_28",
            no_task="if_log_required_employee_type_uri_26_present_29",
        )

        insert_to_list_28 = rail.SetVariableOperator(
            task_id='insert_to_list_28',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Employee type not updated since the employee type is not available in Replicon"
            }
        )

        if_log_required_employee_type_uri_26_present_29 = rail.IfOperator(
            task_id='if_log_required_employee_type_uri_26_present_29',
            test='''{{ result('log_required_employee_type_uri_26') | is_truthy }}''',
            yes_task="update_employee_type_for_user_30",
            no_task="if_log_employeetypenamederived_23_equals_to_hourlyemployee_32",
        )

        update_employee_type_for_user_30 = rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user_30',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeTypeUri": "{{ result('log_required_employee_type_uri_26') }}"
            }
        )

        insert_to_list_31 = rail.SetVariableOperator(
            task_id='insert_to_list_31',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Employee type updated"
            }
        )

        if_log_employeetypenamederived_23_equals_to_hourlyemployee_32 = rail.IfOperator(
            task_id='if_log_employeetypenamederived_23_equals_to_hourlyemployee_32',
            test='''{{ result('log_employeetypenamederived_23') == 'Hourly Employee' }}''',
            yes_task="log_existing_payruleschedule_33",
            no_task="if_request_position_present_59",
        )

        log_existing_payruleschedule_33 = rail.PythonOperator(
            task_id='log_existing_payruleschedule_33',
            python_callable=lambda:  rail.result('bulk_get_users3_6')[
                0]['payRuleScriptSchedule']
        )

        def get_datetime_obj(effectiveDate):
            year = effectiveDate['year']
            month = effectiveDate['month']
            day = effectiveDate['day']
            return datetime.strptime(f"{year}/{month}/{day}", '%Y/%m/%d')

        def payrule_script_data():
            pay_schedules = []
            payrule_schedules = rail.result('bulk_get_users3_6')[
                0]['payRuleScriptSchedule']
            for payrule_schedule in payrule_schedules:
                if payrule_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        payrule_schedule['effectiveDate'])
                    tom_date = pendulum.now(
                        config.pacific_timezone) + timedelta(days=1)
                    if effective_date.date() < tom_date.date():
                        pay_schedules.append({
                            "uri": payrule_schedule['payRuleScript']['uri'],
                            "name": payrule_schedule['payRuleScript']['displayText'],
                            "date": effective_date.strftime('%d/%m/%Y')
                        })
                else:
                    user_start = get_datetime_obj(rail.result('bulk_get_users3_6')[
                        0]['userDetails']['employmentDateRange']['startDate'])
                    pay_schedules.append({
                        "uri": payrule_schedule['payRuleScript']['uri'],
                        "name": payrule_schedule['payRuleScript']['displayText'],
                        "date": user_start.strftime('%d/%m/%Y')
                    })

            return pay_schedules

        log_payruleschedule_42 = rail.PythonOperator(
            task_id='log_payruleschedule_42',
            python_callable=payrule_script_data
        )

        if_first_uri_present_43 = rail.IfOperator(
            task_id='if_first_uri_present_43',
            test='''{{ result('log_payruleschedule_42') | is_truthy }}''',
            yes_task="log_maxeffectivedate_44",
            no_task="log_payrulenamebasedoncurrentpayfrequencyreceived_46",
        )

        log_maxeffectivedate_44 = rail.PythonOperator(
            task_id='log_maxeffectivedate_44',
            python_callable=lambda:  (max(
                datetime.strptime(x['date'], '%d/%m/%Y') for x in rail.result('log_payruleschedule_42'))).strftime('%d/%m/%Y') if rail.result('log_payruleschedule_42') else None
        )

        log_latest_payrulenameassigned_45 = rail.PythonOperator(
            task_id='log_latest_payrulenameassigned_45',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_payruleschedule_42'), 'date', rail.result('log_maxeffectivedate_44'), 'name')
        )

        def get_payrule_from_mapper(dag_run):
            entity_types = list(filter(
                lambda x: x['default'] == "default"
                and x['type'] == "payrule"
                and x['defaulturi'].lower() == dag_run.conf['payfrequency'].lower(), american_integration_default_users_mapper))
            return entity_types[0]['value'] if entity_types else None

        log_payrulenamebasedoncurrentpayfrequencyreceived_46 = rail.PythonOperator(
            task_id='log_payrulenamebasedoncurrentpayfrequencyreceived_46',
            python_callable=get_payrule_from_mapper
        )

        if_log_latest_payrulenameassigned_45_blank_47 = rail.IfOperator(
            task_id='if_log_latest_payrulenameassigned_45_blank_47',
            test='''{{ result('log_latest_payrulenameassigned_45') | is_falsy  or result('log_latest_payrulenameassigned_45') != result('log_payrulenamebasedoncurrentpayfrequencyreceived_46') }}''',
            yes_task="get_all_scripts_payrule_48",
            no_task="if_request_position_present_59",
        )

        get_all_scripts_payrule_48 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_payrule_48',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        declare_list_49 = rail.SetVariableOperator(
            task_id='declare_list_49',
            append=False,
            name='new payrule list to assign',
            value=[]
        )

        def get_current_payrule_script_data():
            pay_schedules = []
            payrule_schedules = rail.result('bulk_get_users3_6')[
                0]['payRuleScriptSchedule']
            for payrule_schedule in payrule_schedules:
                if payrule_schedule['effectiveDate']:
                    pay_schedules.append({
                        "payRuleScript": {
                            "uri": payrule_schedule['payRuleScript']['uri'],
                            "name": null
                        },
                        "effectiveDate": payrule_schedule['effectiveDate']
                    })
                else:
                    pay_schedules.append({
                        "payRuleScript": {
                            "uri": payrule_schedule['payRuleScript']['uri'],
                            "name": null
                        },
                        "effectiveDate": null
                    })
            required_payrule_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_scripts_payrule_48'), 'displayText', rail.result('log_payrulenamebasedoncurrentpayfrequencyreceived_46'), 'uri', '')
            pay_schedules.append({
                "payRuleScript": {
                    "uri": required_payrule_uri,
                    "name": null
                },
                "effectiveDate": {
                    "year": pendulum.now(config.pacific_timezone).year,
                    "month": pendulum.now(config.pacific_timezone).month,
                    "day": pendulum.now(config.pacific_timezone).day
                }
            })

            return pay_schedules

        log_finalpayruletobeassigned_56 = rail.PythonOperator(
            task_id='log_finalpayruletobeassigned_56',
            python_callable=get_current_payrule_script_data
        )

        assignpayrule_57 = rail.RepliconServiceOperator(
            task_id='assignpayrule_57',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('log_finalpayruletobeassigned_56')
            }
        )

        insert_to_list_58 = rail.SetVariableOperator(
            task_id='insert_to_list_58',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Pay rule updated"
            }
        )

        if_request_position_present_59 = rail.IfOperator(
            task_id='if_request_position_present_59',
            test='''{{ dag_run.conf.position | is_truthy }}''',
            yes_task="get_all_departmentswithcode_60",
            no_task="if_declare_variable_4_value_equals_to_yes_71",
        )

        get_all_departmentswithcode_60 = rail.RepliconServiceOperator(
            task_id='get_all_departmentswithcode_60',
            endpoint="/services/DepartmentListService1.svc/GetData",
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

        log_required_department_uri_64 = rail.PythonOperator(
            task_id='log_required_department_uri_64',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_departmentswithcode_60'), 'code', dag_run.conf['position'], 'uri') if rail.result('get_all_departmentswithcode_60') else None
        )

        if_log_required_department_uri_64_blank_65 = rail.IfOperator(
            task_id='if_log_required_department_uri_64_blank_65',
            test='''{{ result('log_required_department_uri_64') | is_falsy }}''',
            yes_task="insert_to_list_66",
            no_task="if_log_required_department_uri_64_not_equals_to_bulk_get_users3_departmenturi_67",
        )

        insert_to_list_66 = rail.SetVariableOperator(
            task_id='insert_to_list_66',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Department not updated since the department is not available in Replicon"
            }
        )

        if_log_required_department_uri_64_not_equals_to_bulk_get_users3_departmenturi_67 = rail.IfOperator(
            task_id='if_log_required_department_uri_64_not_equals_to_bulk_get_users3_departmenturi_67',
            test='''{{ result('log_required_department_uri_64') != result('bulk_get_users3_6')[0].userDetails.department.uri }}''',
            yes_task="update_department_for_user_68",
            no_task="if_declare_variable_4_value_equals_to_yes_71",
        )

        update_department_for_user_68 = rail.RepliconServiceOperator(
            task_id='update_department_for_user_68',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "departmentUri": "{{ result('log_required_department_uri_64') }}"
            }
        )

        insert_to_list_69 = rail.SetVariableOperator(
            task_id='insert_to_list_69',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Department updated"
            }
        )

        update_variable_70 = rail.SetVariableOperator(
            task_id='update_variable_70',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value='yes'
        )

        if_declare_variable_4_value_equals_to_yes_71 = rail.IfOperator(
            task_id='if_declare_variable_4_value_equals_to_yes_71',
            test=lambda: bool(rail.get_dag_run_var(
                rail.result('declare_variable_4')['name']) == 'yes'),
            yes_task="get_all_permission_sets_72",
            no_task="if_request_firstname_present_84",
        )

        get_all_permission_sets_72 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_72',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        def get_permissions_from_mapper(dag_run):
            entity_types = list(filter(
                lambda x: x['default'] == "default"
                and x['type'] == "permission"
                and x['value'].lower() == dag_run.conf['position'].lower(), american_integration_default_users_mapper))
            return [permission_info['defaulturi'] for permission_info in entity_types] if entity_types else []

        log_required_user_permission_set_73 = rail.PythonOperator(
            task_id='log_required_user_permission_set_73',
            python_callable=get_permissions_from_mapper
        )

        def get_permission_uri():
            mapper_permissions = []
            for permission_name in rail.result('log_required_user_permission_set_73'):
                permission_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permission_sets_72'), 'displayText', permission_name, 'uri')
                if permission_uri:
                    mapper_permissions.append(
                        {
                            'name': permission_name,
                            'uri': permission_uri
                        })
            return mapper_permissions

        parse_json_74 = rail.PythonOperator(
            task_id='parse_json_74',
            python_callable=get_permission_uri
        )

        foreach_declare_list_75_78 = rail.ForEachOperator(
            task_id='foreach_declare_list_75_78',
            items="{{ result('parse_json_74') | to_json }}",
            start_task='if_foreach_declare_list_75_78_uri_present_79',
            end_task='foreach_declare_list_75_78_end'
        )

        if_foreach_declare_list_75_78_uri_present_79 = rail.IfOperator(
            task_id='if_foreach_declare_list_75_78_uri_present_79',
            test='''{{ result('foreach_declare_list_75_78').uri | is_truthy }}''',
            yes_task="assign_permission_set_to_user_80",
            no_task="insert_to_list_83",
        )

        assign_permission_set_to_user_80 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_80',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ result('foreach_declare_list_75_78').uri }}"
            }
        )

        insert_to_list_81 = rail.SetVariableOperator(
            task_id='insert_to_list_81',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Permission set - {{ result('foreach_declare_list_75_78').name }} assigned to the user"
            }
        )

        insert_to_list_83 = rail.SetVariableOperator(
            task_id='insert_to_list_83',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Permission set - {{ result('foreach_declare_list_75_78').name }} not assigned to the user since it is not found in Replicon"
            }
        )

        foreach_declare_list_75_78_end = rail.EmptyOperator(
            task_id='foreach_declare_list_75_78_end',
        )

        if_request_firstname_present_84 = rail.IfOperator(
            task_id='if_request_firstname_present_84',
            test='''{{ dag_run.conf.firstname | is_truthy and dag_run.conf.firstname | lower != result('bulk_get_users3_6')[0].userDetails.firstName | lower }}''',
            yes_task="update_first_name_85",
            no_task="if_request_lastname_present_87",
        )

        update_first_name_85 = rail.RepliconServiceOperator(
            task_id='update_first_name_85',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        insert_to_list_86 = rail.SetVariableOperator(
            task_id='insert_to_list_86',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "First name updated"
            }
        )

        if_request_lastname_present_87 = rail.IfOperator(
            task_id='if_request_lastname_present_87',
            test='''{{ dag_run.conf.lastname | is_truthy and dag_run.conf.lastname | lower != result('bulk_get_users3_6')[0].userDetails.lastName | lower }}''',
            yes_task="lastname_88",
            no_task="if_request_email_present_90",
        )

        lastname_88 = rail.RepliconServiceOperator(
            task_id='lastname_88',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        insert_to_list_89 = rail.SetVariableOperator(
            task_id='insert_to_list_89',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Last name updated"
            }
        )

        def email_validation(dag_run):
            existing_email = rail.result('bulk_get_users3_6')[0]['userDetails']['emailAddress'].lower(
            ) if rail.result('bulk_get_users3_6')[0]['userDetails']['emailAddress'] else None
            if dag_run.conf['email']:
                if dag_run.conf['email'].lower() != existing_email:
                    return True
            return False

        if_request_email_present_90 = rail.IfOperator(
            task_id='if_request_email_present_90',
            test=email_validation,
            yes_task="update_email_91",
            no_task="if_request_hiredate_present_93",
        )

        update_email_91 = rail.RepliconServiceOperator(
            task_id='update_email_91',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.email }}"
            }
        )

        insert_to_list_92 = rail.SetVariableOperator(
            task_id='insert_to_list_92',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Email updated"
            }
        )

        if_request_hiredate_present_93 = rail.IfOperator(
            task_id='if_request_hiredate_present_93',
            test='''{{ dag_run.conf.hiredate | is_truthy }}''',
            yes_task="if_startdate_day_present_94",
            no_task="declare_variable_system_default_100",
        )

        if_startdate_day_present_94 = rail.IfOperator(
            task_id='if_startdate_day_present_94',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.day | is_truthy }}''',
            yes_task="log_existing_95",
            no_task="if_hiredate_to_date_not_equals_to_existing_95messagepresent_96",
        )

        log_existing_95 = rail.PythonOperator(
            task_id='log_existing_95',
            python_callable=lambda:  rail.render_template(
                "{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.day }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.month }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.year }}")
        )

        def get_hiredate_validation(dag_run):
            emp_start_date = rail.result('bulk_get_users3_6')[
                0]['userDetails']['employmentDateRange']['startDate']
            employee_start_date = get_datetime_obj(emp_start_date).strftime(
                '%Y-%m-%d') if emp_start_date else '2019-1-1'
            if employee_start_date != dag_run.conf['hiredate']:
                return True
            return False

        if_hiredate_to_date_not_equals_to_existing_95messagepresent_96 = rail.IfOperator(
            task_id='if_hiredate_to_date_not_equals_to_existing_95messagepresent_96',
            test=get_hiredate_validation,
            yes_task="update_employment_date_range_98",
            no_task="declare_variable_system_default_100",
        )

        update_employment_date_range_98 = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_98',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d').year,
                        "month": datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d').month,
                        "day": datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d').day
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        insert_to_list_99 = rail.SetVariableOperator(
            task_id='insert_to_list_99',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Start Date updated"
            }
        )

        declare_variable_system_default_100 = rail.SetVariableOperator(
            task_id='declare_variable_system_default_100',
            append=False,
            name='Timesheet Period uri',
            value=None
        )

        if_request_payfrequency_present_101 = rail.IfOperator(
            task_id='if_request_payfrequency_present_101',
            test='''{{ dag_run.conf.payfrequency | is_truthy and dag_run.conf.payfrequency | lower == 'bi-weekly' }}''',
            yes_task="update_variable_basedon_employeetype_102",
            no_task="if_timesheetperiodtype_uri_not_equals_to_declare_variable_system_default_100value_103",
        )

        update_variable_basedon_employeetype_102 = rail.SetVariableOperator(
            task_id='update_variable_basedon_employeetype_102',
            append=False,
            name='{{ result("declare_variable_system_default_100").name }}',
            value='urn:replicon:timesheet-period-type:based-on-employee-type-assignment'
        )

        if_timesheetperiodtype_uri_not_equals_to_declare_variable_system_default_100value_103 = rail.IfOperator(
            task_id='if_timesheetperiodtype_uri_not_equals_to_declare_variable_system_default_100value_103',
            test='''{{ result('bulk_get_users3_6')[0].timesheetPeriodType.uri != result('declare_variable_system_default_100').value }}''',
            yes_task="update_timesheet_period_type_for_user_104",
            no_task="american_integrated_user_import_logs_add_entry_106",
        )

        update_timesheet_period_type_for_user_104 = rail.RepliconServiceOperator(
            task_id='update_timesheet_period_type_for_user_104',
            endpoint="/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timesheetPeriodTypeUri": "{{ result('declare_variable_system_default_100').value }}"
            }
        )

        insert_to_list_105 = rail.SetVariableOperator(
            task_id='insert_to_list_105',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Timesheet period updated"
            }
        )

        def get_status():
            status = "Skipped"
            exception_info = rail.get_dag_run_var(
                rail.result('declare_list_2')['name'])
            success_info = rail.get_dag_run_var(
                rail.result('declare_list_3')['name'])
            if exception_info:
                status = "Exception"
            if success_info:
                status = "Success"
            return status

        def get_details():
            validation_details = rail.get_dag_run_var(
                rail.result('declare_list_3')['name'])
            success_info = rail.get_dag_run_var(
                rail.result('declare_list_2')['name'])
            exception_info = ""
            if validation_details:
                validations = [val['log'] for val in validation_details]
                exception_info = rail.smartjoin_by_delim(
                    validations, ", ")
            if success_info:
                validations = [val['log'] for val in success_info]
                if exception_info:
                    exception_info = exception_info + ", " + \
                        rail.smartjoin_by_delim(validations, ", ")
                else:
                    exception_info = rail.smartjoin_by_delim(validations, ", ")
            return exception_info

        american_integrated_user_import_logs_add_entry_106 = rail.WriteLogOperator(
            task_id='american_integrated_user_import_logs_add_entry_106',
            message="na",
            # pylint: disable=unnecessary-lambda
            severity=lambda: get_status(),
            properties=lambda dag_run: {
                "Employeeid": dag_run.conf['employeenumber'],
                "Username": dag_run.conf['email'] if dag_run.conf['email'] else f"{dag_run.conf['firstname']} {dag_run.conf['lastname']}",
                "Status": get_status(),
                "Details": get_details(),
                "action": "Update"
            }
        )

        catch_107_107_107 = rail.EmptyOperator(
            task_id='catch_107_107_107',
            trigger_rule='one_failed',
        )

        american_integrated_user_import_logs_add_entry_108 = rail.WriteLogOperator(
            task_id='american_integrated_user_import_logs_add_entry_108',
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "Employeeid": dag_run.conf['employeenumber'],
                "Username": dag_run.conf['email'] if dag_run.conf['email'] else f"{dag_run.conf['firstname']} {dag_run.conf['lastname']}",
                "Status": "Error",
                "action": "Update",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> declare_list_2
        declare_list_2 >> declare_list_3 >> declare_variable_4 >> bulk_get_users3_6 >> if_request_status_contains_inactive_7
        if_request_status_contains_inactive_7 >> rail.Label(
            'Yes') >> disable_login_8 >> american_integrated_user_import_logs_add_entry_9 >> catch_107_107_107
        if_request_status_contains_inactive_7 >> rail.Label(
            'No') >> if_request_status_contains_inactive_11
        if_request_status_contains_inactive_11 >> rail.Label(
            'Yes') >> american_integrated_user_import_logs_add_entry_12 >> catch_107_107_107
        if_request_status_contains_inactive_11 >> rail.Label(
            'No') >> if_request_status_contains_active_14
        if_request_status_contains_active_14 >> rail.Label(
            'Yes') >> enable_login_15 >> american_integration_default_mapper_users_search_entries_16
        if_request_status_contains_active_14 >> rail.Label(
            'No') >> american_integration_default_mapper_users_search_entries_16 >> log_timesheet_templatename_17 >> \
            if_timesheettemplate_name_not_equals_to_dataloggerlog_timesheet_templatename_17message_18
        if_timesheettemplate_name_not_equals_to_dataloggerlog_timesheet_templatename_17message_18 >> rail.Label(
            'Yes') >> get_all_policy_sets_19 >> log_timesheettemplateuri_20 >> \
            assign_timesheet_template_21 >> insert_to_list_22 >> log_employeetypenamederived_23
        if_timesheettemplate_name_not_equals_to_dataloggerlog_timesheet_templatename_17message_18 >> rail.Label(
            'No') >> log_employeetypenamederived_23 >> if_request_payfrequency_present_24
        if_request_payfrequency_present_24 >> rail.Label(
            'Yes') >> _adhoc_http_action_25 >> log_required_employee_type_uri_26 >> \
            if_log_required_employee_type_uri_26_blank_27
        if_log_required_employee_type_uri_26_blank_27 >> rail.Label(
            'Yes') >> insert_to_list_28 >> if_log_required_employee_type_uri_26_present_29
        if_log_required_employee_type_uri_26_blank_27 >> rail.Label(
            'No') >> if_log_required_employee_type_uri_26_present_29
        if_log_required_employee_type_uri_26_present_29 >> rail.Label(
            'Yes') >> update_employee_type_for_user_30 >> insert_to_list_31 >> \
            if_log_employeetypenamederived_23_equals_to_hourlyemployee_32
        if_log_required_employee_type_uri_26_present_29 >> rail.Label(
            'No') >> if_log_employeetypenamederived_23_equals_to_hourlyemployee_32
        if_log_employeetypenamederived_23_equals_to_hourlyemployee_32 >> rail.Label(
            'Yes') >> log_existing_payruleschedule_33 >> log_payruleschedule_42 >> if_first_uri_present_43
        if_first_uri_present_43 >> rail.Label(
            'Yes') >> log_maxeffectivedate_44 >> log_latest_payrulenameassigned_45 >> log_payrulenamebasedoncurrentpayfrequencyreceived_46
        if_first_uri_present_43 >> rail.Label(
            'No') >> log_payrulenamebasedoncurrentpayfrequencyreceived_46 >> if_log_latest_payrulenameassigned_45_blank_47
        if_log_latest_payrulenameassigned_45_blank_47 >> rail.Label(
            'Yes') >> get_all_scripts_payrule_48 >> declare_list_49 >> \
            log_finalpayruletobeassigned_56 >> assignpayrule_57 >> insert_to_list_58 >> if_request_position_present_59
        if_log_latest_payrulenameassigned_45_blank_47 >> rail.Label(
            'No') >> if_request_position_present_59
        if_log_employeetypenamederived_23_equals_to_hourlyemployee_32 >> rail.Label(
            'No') >> if_request_position_present_59
        if_request_payfrequency_present_24 >> rail.Label(
            'No') >> if_request_position_present_59
        if_request_position_present_59 >> rail.Label(
            'Yes') >> get_all_departmentswithcode_60 >> log_required_department_uri_64 >> if_log_required_department_uri_64_blank_65
        if_log_required_department_uri_64_blank_65 >> rail.Label(
            'Yes') >> insert_to_list_66 >> if_log_required_department_uri_64_not_equals_to_bulk_get_users3_departmenturi_67
        if_log_required_department_uri_64_blank_65 >> rail.Label(
            'No') >> if_log_required_department_uri_64_not_equals_to_bulk_get_users3_departmenturi_67
        if_log_required_department_uri_64_not_equals_to_bulk_get_users3_departmenturi_67 >> rail.Label(
            'Yes') >> update_department_for_user_68 >> insert_to_list_69 >> \
            update_variable_70 >> if_declare_variable_4_value_equals_to_yes_71
        if_log_required_department_uri_64_not_equals_to_bulk_get_users3_departmenturi_67 >> rail.Label(
            'No') >> if_declare_variable_4_value_equals_to_yes_71
        if_request_position_present_59 >> rail.Label(
            'No') >> if_declare_variable_4_value_equals_to_yes_71
        if_declare_variable_4_value_equals_to_yes_71 >> rail.Label(
            'Yes') >> get_all_permission_sets_72 >> log_required_user_permission_set_73 >> parse_json_74 >> \
            foreach_declare_list_75_78 >> if_foreach_declare_list_75_78_uri_present_79
        if_foreach_declare_list_75_78_uri_present_79 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_80 >> insert_to_list_81 >> foreach_declare_list_75_78_end
        if_foreach_declare_list_75_78_uri_present_79 >> rail.Label(
            'No') >> insert_to_list_83 >> foreach_declare_list_75_78_end
        foreach_declare_list_75_78 >> foreach_declare_list_75_78_end >> if_request_firstname_present_84
        if_declare_variable_4_value_equals_to_yes_71 >> rail.Label(
            'No') >> if_request_firstname_present_84
        if_request_firstname_present_84 >> rail.Label(
            'Yes') >> update_first_name_85 >> insert_to_list_86 >> if_request_lastname_present_87
        if_request_firstname_present_84 >> rail.Label(
            'No') >> if_request_lastname_present_87
        if_request_lastname_present_87 >> rail.Label(
            'Yes') >> lastname_88 >> insert_to_list_89 >> if_request_email_present_90
        if_request_lastname_present_87 >> rail.Label(
            'No') >> if_request_email_present_90
        if_request_email_present_90 >> rail.Label(
            'Yes') >> update_email_91 >> insert_to_list_92 >> if_request_hiredate_present_93
        if_request_email_present_90 >> rail.Label(
            'No') >> if_request_hiredate_present_93
        if_request_hiredate_present_93 >> rail.Label(
            'Yes') >> if_startdate_day_present_94
        if_startdate_day_present_94 >> rail.Label(
            'Yes') >> log_existing_95 >> if_hiredate_to_date_not_equals_to_existing_95messagepresent_96
        if_startdate_day_present_94 >> rail.Label(
            'No') >> if_hiredate_to_date_not_equals_to_existing_95messagepresent_96
        if_hiredate_to_date_not_equals_to_existing_95messagepresent_96 >> rail.Label(
            'Yes') >> update_employment_date_range_98 >> insert_to_list_99 >> declare_variable_system_default_100
        if_hiredate_to_date_not_equals_to_existing_95messagepresent_96 >> rail.Label(
            'No') >> declare_variable_system_default_100
        if_request_hiredate_present_93 >> rail.Label(
            'No') >> declare_variable_system_default_100 >> if_request_payfrequency_present_101
        if_request_payfrequency_present_101 >> rail.Label(
            'Yes') >> update_variable_basedon_employeetype_102 >> if_timesheetperiodtype_uri_not_equals_to_declare_variable_system_default_100value_103
        if_request_payfrequency_present_101 >> rail.Label(
            'No') >> if_timesheetperiodtype_uri_not_equals_to_declare_variable_system_default_100value_103
        if_timesheetperiodtype_uri_not_equals_to_declare_variable_system_default_100value_103 >> rail.Label(
            'Yes') >> update_timesheet_period_type_for_user_104 >> insert_to_list_105 >> american_integrated_user_import_logs_add_entry_106
        if_timesheetperiodtype_uri_not_equals_to_declare_variable_system_default_100value_103 >> rail.Label(
            'No') >> american_integrated_user_import_logs_add_entry_106 >> catch_107_107_107 >> \
            american_integrated_user_import_logs_add_entry_108 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)


from datetime import timedelta, datetime
import itertools
import pendulum
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
from ge.user_sync_slovakia.slovakia_master_mapper import slovakia_master_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_slovakia_add_v1_0_{config.instance}',
        description=f'GE slovakia Add V1.0 {config.instance}',
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
            no_task='declare_list_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_2',
            end_task='ey_user_import_logs_add_entry_141',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_2 = rail.SetVariableOperator(
            task_id='declare_list_2',
            append=False,
            name='exception logger',
            value=[]
        )

        def get_validation_info(dag_run):
            validation_message = []
            if dag_run.conf['EmployeeFirstName'] is None:
                validation_message.append('Employee First  Name not present')
            if dag_run.conf['EmployeeLastName'] is None:
                validation_message.append('Employee Last  Name not present')
            if dag_run.conf['OHRID'] is None:
                validation_message.append('OHRID not present')
            if dag_run.conf['LegalEntityHireDate'] and "/" not in dag_run.conf['LegalEntityHireDate']:
                validation_message.append(
                    'Legal Entry Hire date not in allowed format')
            if dag_run.conf['LegalEntityHireDate'] is None and dag_run.conf['HireEffectiveDate'] and "/" not in dag_run.conf['HireEffectiveDate']:
                validation_message.append(
                    'Hire Effective date not in allowed format')
            if dag_run.conf['LegalEntityHireDate'] is None and dag_run.conf['HireEffectiveDate'] is None:
                validation_message.append(
                    'Legal Entity Hire Date or Hire Effective date is not present')
            if dag_run.conf['LegalEntity'] is None:
                validation_message.append('Legal Entity not present')
            return rail.smartjoin_by_delim(validation_message, ';')

        log_checkifrequiredfieldsarenotthere_4 = rail.PythonOperator(
            task_id='log_checkifrequiredfieldsarenotthere_4',
            python_callable=get_validation_info
        )

        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 = rail.IfOperator(
            task_id='if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5',
            test='''{{ result('log_checkifrequiredfieldsarenotthere_4') | is_truthy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_6",
            no_task="slovakia_master_mapper_search_entries_8",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_6 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_6',
            message="na",
            severity="Skipped",
            properties={
                "action": "Add",
                "status": "Skipped",
                "child_job_id": "{{ dag_run_ecid() }}",
                "details": '{{result("log_checkifrequiredfieldsarenotthere_4")}}',
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        slovakia_master_mapper_search_entries_8 = rail.PythonOperator(
            task_id='slovakia_master_mapper_search_entries_8',
            python_callable=lambda dag_run: list(
                filter(lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'], slovakia_master_mapper))
        )

        if_first_id_blank_9 = rail.IfOperator(
            task_id='if_first_id_blank_9',
            test='''{{ result('slovakia_master_mapper_search_entries_8') | is_falsy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_10",
            no_task="log_employee_type_name_from_mapper_12",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_10 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_10',
            message="na",
            severity="Skipped",
            properties={
                "action": "Add",
                "status": "Skipped",
                "child_job_id": "{{ dag_run_ecid() }}",
                "details": 'Legal Entity is not available in Mapper',
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        def get_employe_type_from_mapper(dag_run):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Employee Type", slovakia_master_mapper))
            return emp_types[0]['value'] if emp_types else None

        log_employee_type_name_from_mapper_12 = rail.PythonOperator(
            task_id='log_employee_type_name_from_mapper_12',
            python_callable=get_employe_type_from_mapper
        )

        if_log_employee_type_name_from_mapper_12_blank_13 = rail.IfOperator(
            task_id='if_log_employee_type_name_from_mapper_12_blank_13',
            test='''{{ result('log_employee_type_name_from_mapper_12') | is_falsy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_14",
            no_task="_adhoc_http_action_16",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_14 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_14',
            message="na",
            severity="Skipped",
            properties={
                "action": "Add",
                "status": "Skipped",
                "details": 'Employee type is not available in Mapper for Legal Entity {{ dag_run.conf.LegalEntity }}',
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        _adhoc_http_action_16 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_16',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails"
        )

        def get_employee_type_uri():
            current_employeetype = list(filter(lambda x: x['name'] and x['name'].lower() == rail.result(
                'log_employee_type_name_from_mapper_12').lower(), rail.result('_adhoc_http_action_16')))
            return current_employeetype[0]['uri'] if current_employeetype else None

        log_required_employee_type_uri_17 = rail.PythonOperator(
            task_id='log_required_employee_type_uri_17',
            python_callable=get_employee_type_uri
        )

        if_log_required_employee_type_uri_17_blank_18 = rail.IfOperator(
            task_id='if_log_required_employee_type_uri_17_blank_18',
            test='''{{ result('log_required_employee_type_uri_17') | is_falsy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_19",
            no_task="log_required_department_name_21",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_19 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_19',
            message="na",
            severity="Skipped",
            properties={
                "action": "Add",
                "status": "Skipped",
                "details": '''Employee type {{result('log_employee_type_name_from_mapper_12')}} is not available in Replicon''',
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        def get_entity_from_mapper(dag_run, entity_type):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == entity_type, slovakia_master_mapper))
            return emp_types[0]['value'] if emp_types else None

        def get_value_from_mapper(dag_run, entity_type1, entity_type2):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['type'] == entity_type1
                and x['identifier_1_(_legal_entity_code/_type/_timeoff_type)'] == entity_type2, slovakia_master_mapper))
            return emp_types[0]['value'] if emp_types else None

        def get_entity_uri_from_mapper(dag_run, entity_type):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == entity_type, slovakia_master_mapper))
            return emp_types[0]['default_uri'] if emp_types else None

        log_required_department_name_21 = rail.PythonOperator(
            task_id='log_required_department_name_21',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Department")
        )

        if_log_37_blank_22 = rail.IfOperator(
            task_id='if_log_37_blank_22',
            test='''{{ result('log_required_department_name_21') | is_falsy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_23",
            no_task="_adhoc_http_action_25",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_23 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_23',
            message="na",
            severity="Skipped",
            properties={
                "action": "Add",
                "status": "Skipped",
                "details": '''Department is not available in Mapper for Legal Entity {{ dag_run.conf.LegalEntity }}''',
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        _adhoc_http_action_25 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_25',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments"
        )

        def get_department_uri():
            current_department = list(filter(lambda x: x['name'] and x['name'].lower() == rail.result(
                'log_required_department_name_21').lower(), rail.result('_adhoc_http_action_25')))
            return current_department[0]['uri'] if current_department else None

        log_required_department_uri_26 = rail.PythonOperator(
            task_id='log_required_department_uri_26',
            python_callable=get_department_uri
        )

        if_log_required_department_uri_26_blank_27 = rail.IfOperator(
            task_id='if_log_required_department_uri_26_blank_27',
            test='''{{ result('log_required_department_uri_26') | is_falsy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_28",
            no_task="log_required_timesheet_period_name_30",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_28 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_28',
            message="na",
            severity="Skipped",
            properties={
                "action": "{{ dag_run.conf.type }}",
                "status": "Skipped",
                "details": '''Department {{result('log_required_department_name_21')}} is not available in Replicon''',
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        log_required_timesheet_period_name_30 = rail.PythonOperator(
            task_id='log_required_timesheet_period_name_30',
            python_callable=lambda dag_run: get_entity_uri_from_mapper(
                dag_run, "Timesheet Period")
        )

        log_required_time_off_template_name_31 = rail.PythonOperator(
            task_id='log_required_time_off_template_name_31',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Timeoff Template")
        )

        log_required_time_off_approval_path_name_32 = rail.PythonOperator(
            task_id='log_required_time_off_approval_path_name_32',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Timeoff Approval Path")
        )

        log_required_timesheet_approval_path_name_33 = rail.PythonOperator(
            task_id='log_required_timesheet_approval_path_name_33',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Timesheet Approval Path")
        )

        log_valueforpayrulederivation_34 = rail.PythonOperator(
            task_id='log_valueforpayrulederivation_34',
            python_callable=lambda dag_run: dag_run.conf['JobPositionTitle'] if dag_run.conf['JobPositionTitle'] in [
                'Field Engineer 2', 'Engineer - Remote Technical Support'] else "NA"
        )

        log_required_payrule_name_35 = rail.PythonOperator(
            task_id='log_required_payrule_name_35',
            python_callable=lambda dag_run: get_value_from_mapper(
                dag_run, "Payrule", rail.result('log_valueforpayrulederivation_34'))
        )

        log_required_holiday_calendar_name_36 = rail.PythonOperator(
            task_id='log_required_holiday_calendar_name_36',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Holiday Calendar")
        )

        log_required_workweek_name_37 = rail.PythonOperator(
            task_id='log_required_workweek_name_37',
            python_callable=lambda dag_run: get_entity_uri_from_mapper(
                dag_run, "Work Week")
        )

        log_required_authentication_type_38 = rail.PythonOperator(
            task_id='log_required_authentication_type_38',
            python_callable=lambda dag_run: get_entity_uri_from_mapper(
                dag_run, "Authentication type")
        )

        log_required_user_permission_set_39 = rail.PythonOperator(
            task_id='log_required_user_permission_set_39',
            python_callable=lambda dag_run: get_value_from_mapper(
                dag_run, "Permission", "User")
        )

        log_required_legal_entity_name_40 = rail.PythonOperator(
            task_id='log_required_legal_entity_name_40',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Legal Entity")
        )

        log_required_location_name_41 = rail.PythonOperator(
            task_id='log_required_location_name_41',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Location")
        )

        invoke_custom_ruby_code_42 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_42',
            python_callable=lambda dag_run: dag_run.conf[
                'LegalEntityHireDate'] or dag_run.conf['HireEffectiveDate']
        )

        create_user_43 = rail.RepliconServiceOperator(
            task_id='create_user_43',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['OHRID'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['EmployeeFirstName'],
                    "lastname": dag_run.conf['EmployeeLastName'],
                    "emailAddress": dag_run.conf['EmployeeEmailAddress'],
                    "employeeId": dag_run.conf['OHRID'],
                    "department": {
                        "uri": rail.result('log_required_department_uri_26'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": rail.result('log_required_workweek_name_37'),
                    "employmentDateRange": {
                        "startDate": {
                            "year": datetime.strptime(rail.result('invoke_custom_ruby_code_42'), '%d/%m/%Y').year,
                            "month": datetime.strptime(rail.result('invoke_custom_ruby_code_42'), '%d/%m/%Y').month,
                            "day": datetime.strptime(rail.result('invoke_custom_ruby_code_42'), '%d/%m/%Y').day,
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            rail.result('log_required_authentication_type_38')
                        ],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['OHRID'],
                        "SSOName": dag_run.conf['OHRID'],
                        "password": null
                    },
                    "holidayCalendar": {
                        "uri": null,
                        "name": rail.result('log_required_holiday_calendar_name_36')
                    },
                    "timeOffPolicy": null,
                    "permissionSets": [
                        {
                            "uri": null,
                            "name": rail.result('log_required_user_permission_set_39')
                        }
                    ],
                    "policySets": [
                        {
                            "uri": null,
                            "name": rail.result('log_required_time_off_template_name_31')
                        }
                    ],
                    "employeeType": {
                        "uri": rail.result('log_required_employee_type_uri_17'),
                        "name": null
                    },
                    "timesheetPeriodTypeUri": rail.result('log_required_timesheet_period_name_30'),
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {
                        "uri": null,
                        "name": rail.result('log_required_timesheet_approval_path_name_33')
                    },
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": {
                        "uri": null,
                        "name": rail.result('log_required_time_off_approval_path_name_32')
                    },
                    "customFieldValues": [],
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [
                        {
                            "location": {
                                "uri": null,
                                "parentUri": null,
                                "name": rail.result('log_required_location_name_41')
                            },
                            "effectiveDate": null
                        }
                    ],
                    "divisionSchedule": [],
                    "costCenterSchedule": [
                        {
                            "costCenter": {
                                "uri": null,
                                "parentUri": null,
                                "name": rail.result('log_required_legal_entity_name_40')
                            },
                            "effectiveDate": null
                        }
                    ],
                    "serviceCenterSchedule": [],
                    "departmentGroupSchedule": [],
                    "employeeTypeGroupSchedule": [],
                    "timesheetPeriodSchedule": [],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": [
                        {
                            "payRuleScript": {
                                "uri": null,
                                "name": rail.result('log_required_payrule_name_35')
                            },
                            "effectiveDate": null
                        }
                    ]
                }
            }
        )

        remove_timeoff_assignments_44 = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignments_44',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_43').uri }}",
                "timeOffTypeUris": []
            }
        )

        def get_mapper_licenses(dag_run, typeval):
            employee_licenses = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['type'] == typeval, slovakia_master_mapper))
            licenses = [licenses['default_uri']
                        for licenses in employee_licenses]
            return rail.smartjoin_by_delim(licenses, ',')

        log_required_licenses_45 = rail.PythonOperator(
            task_id='log_required_licenses_45',
            python_callable=lambda dag_run: get_mapper_licenses(
                dag_run, "License")
        )

        put_product_assignments_for_user_46 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_46',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_43')['uri'],
                "productUris": rail.result('log_required_licenses_45').split(',')
            }
        )

        log_required_timesheet_template_47 = rail.PythonOperator(
            task_id='log_required_timesheet_template_47',
            python_callable=lambda dag_run: get_value_from_mapper(
                dag_run, "Timesheet Template", rail.result('log_valueforpayrulederivation_34'))
        )

        if_log_required_timesheet_template_47_blank_48 = rail.IfOperator(
            task_id='if_log_required_timesheet_template_47_blank_48',
            test='''{{ result('log_required_timesheet_template_47') | is_falsy }}''',
            yes_task="insert_to_list_49",
            no_task="_adhoc_http_action_51",
        )

        insert_to_list_49 = rail.SetVariableOperator(
            task_id='insert_to_list_49',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": '''Timesheet template not available in mapper for legal entity {{dag_run.conf.LegalEntity}}'''
            }
        )

        _adhoc_http_action_51 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_51',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        def get_tstemplate_uri():
            current_template = list(filter(lambda x: x['name'] and x['name'].lower() == rail.result(
                'log_required_timesheet_template_47').lower(), rail.result('_adhoc_http_action_51')))
            return current_template[0]['uri'] if current_template else None

        log_required_timesheet_template_uri_52 = rail.PythonOperator(
            task_id='log_required_timesheet_template_uri_52',
            python_callable=get_tstemplate_uri
        )

        if_log_required_timesheet_template_uri_52_present_53 = rail.IfOperator(
            task_id='if_log_required_timesheet_template_uri_52_present_53',
            test='''{{ result('log_required_timesheet_template_uri_52') | is_truthy }}''',
            yes_task="assign_policy_set_to_user_54",
            no_task="insert_to_list_56",
        )

        assign_policy_set_to_user_54 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_54',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ result('create_user_43').uri }}",
                "policySetUri": "{{ result('log_required_timesheet_template_uri_52') }}"
            }
        )

        insert_to_list_56 = rail.SetVariableOperator(
            task_id='insert_to_list_56',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": '''Timesheet template {{result('log_required_timesheet_template_47')}} not available in Replicon'''
            }
        )

        if_request_supervisorssoid_present_57 = rail.IfOperator(
            task_id='if_request_supervisorssoid_present_57',
            test='''{{ dag_run.conf.SupervisorSSOID | is_truthy }}''',
            yes_task="if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_58",
            no_task="_adhoc_http_action_82",
        )

        if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_58 = rail.IfOperator(
            task_id='if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_58',
            test='''{{ dag_run.conf.SupervisorSSOID == dag_run.conf.OHRID }}''',
            yes_task="insert_to_list_59",
            no_task="if_request_supervisorssoid_present_61",
        )

        insert_to_list_59 = rail.SetVariableOperator(
            task_id='insert_to_list_59',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Supervisor not assigned since the user and supervisor SSO ID are same."
            }
        )

        if_request_supervisorssoid_present_61 = rail.IfOperator(
            task_id='if_request_supervisorssoid_present_61',
            test='''{{ dag_run.conf.SupervisorSSOID | is_truthy }}''',
            yes_task="search_users_search_supervisor_62",
            no_task="_adhoc_http_action_82",
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def compose_user_details(response, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))
            return users_info[0] if users_info else None

        search_users_search_supervisor_62 = rail.RepliconServicePageOperator(
            task_id='search_users_search_supervisor_62',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                "sort": [],
                "filterExpression": {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['SupervisorSSOID'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['SupervisorSSOID'])
        )

        if_log_supervisor_uri_63_blank_64 = rail.IfOperator(
            task_id='if_log_supervisor_uri_63_blank_64',
            test='''{{ result('search_users_search_supervisor_62') | is_falsy }}''',
            yes_task="ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_65",
            no_task="if_downcase_not_equals_to_true_68",
        )

        ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_65 = rail.WriteLogOperator(
            task_id='ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_65',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "username": dag_run.conf['OHRID'],
                "useruri": rail.result('create_user_43')['uri'],
                "supervisorloginname": dag_run.conf['SupervisorSSOID'],
                "action": "Add",
                "childjobid": get_dagrun_ecid(dag_run),
                "supervisoreffectivedate": pendulum.now(
                    config.pacific_timezone).strftime("%d/%m/%Y"),
                "status": "queued",
                "supervisorusername": dag_run.conf['SupervisorName']
            }
        )

        if_downcase_not_equals_to_true_68 = rail.IfOperator(
            task_id='if_downcase_not_equals_to_true_68',
            test='''{{ result('search_users_search_supervisor_62') | is_truthy and result('search_users_search_supervisor_62').status == 'False' }}''',
            yes_task="ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_69",
            no_task="_adhoc_http_action_search_supervisor_71",
        )

        ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_69 = rail.WriteLogOperator(
            task_id='ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_69',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "username": dag_run.conf['OHRID'],
                "useruri": rail.result('create_user_43')['uri'],
                "supervisorloginname": dag_run.conf['SupervisorSSOID'],
                "action": "Add",
                "childjobid": get_dagrun_ecid(dag_run),
                "supervisoreffectivedate": pendulum.now(
                    config.pacific_timezone).strftime("%d/%m/%Y"),
                "status": "queued",
                "supervisorusername": dag_run.conf['SupervisorName']
            }
        )

        _adhoc_http_action_search_supervisor_71 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_search_supervisor_71',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_search_supervisor_62').useruri }}"
            }
        )

        def get_super_user_permissions(dag_run, entity_type_1, entity_type_2):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['type'] == entity_type_1
                and x['identifier_1_(_legal_entity_code/_type/_timeoff_type)'] == entity_type_2, slovakia_master_mapper))
            return [emp_type['value'] for emp_type in emp_types] if emp_types else []

        log_required_supervisor_permission_72 = rail.PythonOperator(
            task_id='log_required_supervisor_permission_72',
            python_callable=lambda dag_run: get_super_user_permissions(
                dag_run, 'Permission', 'Supervsior')
        )

        def get_permission_type(permission_uri):
            permissionset = rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_search_supervisor_71'), 'policyUri', permission_uri, 'permissionSet')
            return permissionset['name'] if permissionset else None

        log_supervisorpermissionassignedtouser_73 = rail.PythonOperator(
            task_id='log_supervisorpermissionassignedtouser_73',
            python_callable=lambda: get_permission_type(
                'urn:replicon:policy:supervision')
        )

        log_end_userpermissionassignedtouser_74 = rail.PythonOperator(
            task_id='log_end_userpermissionassignedtouser_74',
            python_callable=lambda: get_permission_type(
                'urn:replicon:policy:user')
        )

        def is_valid_permission():
            if rail.result('log_supervisorpermissionassignedtouser_73') is None or rail.result('log_end_userpermissionassignedtouser_74') is None \
                or rail.result('log_supervisorpermissionassignedtouser_73') not in rail.result('log_required_supervisor_permission_72') \
                    or rail.result('log_end_userpermissionassignedtouser_74') not in rail.result('log_required_supervisor_permission_72'):
                return True
            return False

        if_log_supervisorpermissionassignedtouser_73_blank_75 = rail.IfOperator(
            task_id='if_log_supervisorpermissionassignedtouser_73_blank_75',
            test=is_valid_permission,
            yes_task="_adhoc_http_action_search_supervisor_76",
            no_task="assign_initital_supervisor_81",
        )

        _adhoc_http_action_search_supervisor_76 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_search_supervisor_76',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        def get_super_permissions(response, dag_run):
            permissions_to_add = []
            mapper_permissions = get_super_user_permissions(
                dag_run, 'Permission', 'Supervsior')
            if response and mapper_permissions:
                for permission in mapper_permissions:
                    permission_uri = rail.find_first_by_attr_and_get_attr(
                        response, 'name', permission, 'uri')
                    if permission_uri:
                        permissions_to_add.append(permission_uri)
            return permissions_to_add

        get_permission_uris = rail.RepliconServiceOperator(
            task_id='get_permission_uris',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            log_response=True,
            data_handler=get_super_permissions
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_permission_uris') | length > 0 }}",
            yes_task='add_supervisor_permissions',
            no_task='assign_initital_supervisor_81'
        )

        add_supervisor_permissions = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result('get_permission_uris'),
            execution_timeout=timedelta(days=14),
            data={
                'userUri': "{{ result('search_users_search_supervisor_62').useruri }}",
                'permissionSetUri': '{{ item }}'
            }
        )

        # foreach_document_78 = rail.ForEachOperator(
        #     task_id='foreach_document_78',
        #     items=lambda: rail.result('log_required_supervisor_permission_72'),
        #     start_task='log_checkifpermissionisavailable_79',
        #     end_task='foreach_document_78_end'
        # )

        # log_checkifpermissionisavailable_79 = rail.PythonOperator(
        #     task_id='log_checkifpermissionisavailable_79',
        #     python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
        #         '_adhoc_http_action_search_supervisor_76'), 'name', rail.result('foreach_document_78'), 'uri')
        # )

        # assign_permission_set_to_user_80 = rail.RepliconServiceOperator(
        #     task_id='assign_permission_set_to_user_80',
        #     endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
        #     data={
        #         "userUri": "{{ result('search_users_search_supervisor_62').useruri }}",
        #         "permissionSetUri": "{{ result('log_checkifpermissionisavailable_79') }}"
        #     }
        # )

        # foreach_document_78_end = rail.EmptyOperator(
        #     task_id='foreach_document_78_end',
        # )

        assign_initital_supervisor_81 = rail.RepliconServiceOperator(
            task_id='assign_initital_supervisor_81',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_43').uri }}",
                "supervisorUri": "{{ result('search_users_search_supervisor_62').useruri }}",
                "dateRange": null
            }
        )

        _adhoc_http_action_82 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_82',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        if_request_hrmssoid_present_83 = rail.IfOperator(
            task_id='if_request_hrmssoid_present_83',
            test='''{{ dag_run.conf.HRMSSOID | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84",
            no_task="if_request_hrmname_present_89",
        )

        log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_82'), 'displayText', 'HRM SSO ID', 'uri')
        )

        if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84_blank_85 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84_blank_85',
            test='''{{ result('log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84') | is_falsy }}''',
            yes_task="insert_to_list_86",
            no_task="updated_u_d_ffor_h_r_m_s_s_o_i_d_88",
        )

        insert_to_list_86 = rail.SetVariableOperator(
            task_id='insert_to_list_86',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "HRM SSO ID udf is not available"
            }
        )

        updated_u_d_ffor_h_r_m_s_s_o_i_d_88 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_h_r_m_s_s_o_i_d_88',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_43').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84') }}",
                "value": "{{ dag_run.conf.HRMSSOID }}"
            }
        )

        if_request_hrmname_present_89 = rail.IfOperator(
            task_id='if_request_hrmname_present_89',
            test='''{{ dag_run.conf.HRMName | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_h_r_m_name_90",
            no_task="if_request_jobpositiontitle_present_96",
        )

        log_get_u_d_f_uri_for_h_r_m_name_90 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_h_r_m_name_90',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_82'), 'displayText', 'HRM Name', 'uri')
        )

        log_get_u_d_f_uri_for_h_r_m_name_91 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_h_r_m_name_91',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_82'), 'displayText', 'HRM Name', 'uri')
        )

        if_log_get_u_d_f_uri_for_h_r_m_name_90_blank_92 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_h_r_m_name_90_blank_92',
            test='''{{ result('log_get_u_d_f_uri_for_h_r_m_name_90') | is_falsy }}''',
            yes_task="insert_to_list_93",
            no_task="updated_u_d_ffor_h_r_m_name_95",
        )

        insert_to_list_93 = rail.SetVariableOperator(
            task_id='insert_to_list_93',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "HRM Name udf is not available"
            }
        )

        updated_u_d_ffor_h_r_m_name_95 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_h_r_m_name_95',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_43').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_h_r_m_name_90') }}",
                "value": "{{ dag_run.conf.HRMName }}"
            }
        )

        if_request_jobpositiontitle_present_96 = rail.IfOperator(
            task_id='if_request_jobpositiontitle_present_96',
            test='''{{ dag_run.conf.JobPositionTitle | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_job_position_title_97",
            no_task="if_request_suspendassignmentcategory_present_102",
        )

        log_get_u_d_f_uri_for_job_position_title_97 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_job_position_title_97',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_82'), 'displayText', 'Job/Position Title', 'uri')
        )

        if_log_get_u_d_f_uri_for_job_position_title_97_blank_98 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_job_position_title_97_blank_98',
            test='''{{ result('log_get_u_d_f_uri_for_job_position_title_97') | is_falsy }}''',
            yes_task="insert_to_list_99",
            no_task="updated_u_d_ffor_job_position_title_101",
        )

        insert_to_list_99 = rail.SetVariableOperator(
            task_id='insert_to_list_99',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Job/Position Title udf is not available"
            }
        )

        updated_u_d_ffor_job_position_title_101 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_job_position_title_101',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_43').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_job_position_title_97') }}",
                "value": "{{ dag_run.conf.JobPositionTitle }}"
            }
        )

        if_request_suspendassignmentcategory_present_102 = rail.IfOperator(
            task_id='if_request_suspendassignmentcategory_present_102',
            test='''{{ dag_run.conf.SuspendAssignmentCategory | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_suspend_assignment_category_103",
            no_task="if_request_industryfocusgroup_present_113",
        )

        log_get_u_d_f_uri_for_suspend_assignment_category_103 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_suspend_assignment_category_103',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_82'), 'displayText', 'Suspend Assignment Category', 'uri')
        )

        if_log_get_u_d_f_uri_for_suspend_assignment_category_103_blank_104 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_suspend_assignment_category_103_blank_104',
            test='''{{ result('log_get_u_d_f_uri_for_suspend_assignment_category_103') | is_falsy }}''',
            yes_task="insert_to_list_105",
            no_task="_adhoc_http_action_107",
        )

        insert_to_list_105 = rail.SetVariableOperator(
            task_id='insert_to_list_105',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Suspend Assignment Category udf is not available"
            }
        )

        _adhoc_http_action_107 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_107',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_suspend_assignment_category_103') }}"
            }
        )

        log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108 = rail.PythonOperator(
            task_id='log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_107'), 'displayText', dag_run.conf['SuspendAssignmentCategory'], 'uri')
        )

        if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108_blank_109 = rail.IfOperator(
            task_id='if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108_blank_109',
            test='''{{ result('log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108') | is_falsy }}''',
            yes_task="insert_to_list_110",
            no_task="updated_u_d_ffor_suspend_assignment_category_112",
        )

        insert_to_list_110 = rail.SetVariableOperator(
            task_id='insert_to_list_110',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": '''Suspend Assignment Category value {{ dag_run.conf.Assignmentcategory }} is not available in Replicon'''
            }
        )

        updated_u_d_ffor_suspend_assignment_category_112 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_suspend_assignment_category_112',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user_43').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_suspend_assignment_category_103') }}",
                "customFieldDropDownOptionUri": "{{ result('log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108') }}"
            }
        )

        if_request_industryfocusgroup_present_113 = rail.IfOperator(
            task_id='if_request_industryfocusgroup_present_113',
            test='''{{ dag_run.conf.IndustryFocusGroup | is_truthy }}''',
            yes_task="_adhoc_http_action_114",
            no_task="log_office_schedulename_120",
        )

        _adhoc_http_action_114 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_114',
            endpoint="/services/DivisionService1.svc/GetAllDivisions"
        )

        log_gettherequired_industry_focus_groupvalue_uri_115 = rail.PythonOperator(
            task_id='log_gettherequired_industry_focus_groupvalue_uri_115',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_114'), 'displayText', dag_run.conf['IndustryFocusGroup'], 'uri')
        )

        if_log_gettherequired_industry_focus_groupvalue_uri_115_present_116 = rail.IfOperator(
            task_id='if_log_gettherequired_industry_focus_groupvalue_uri_115_present_116',
            test='''{{ result('log_gettherequired_industry_focus_groupvalue_uri_115') | is_truthy }}''',
            yes_task="put_industry_focus_group_schedule_for_user_division_117",
            no_task="insert_to_list_119",
        )

        put_industry_focus_group_schedule_for_user_division_117 = rail.RepliconServiceOperator(
            task_id='put_industry_focus_group_schedule_for_user_division_117',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data={
                "userUri": "{{ result('create_user_43').uri }}",
                "scheduleEntries": [
                    {
                        "division": {
                            "uri": "{{ result('log_gettherequired_industry_focus_groupvalue_uri_115') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        insert_to_list_119 = rail.SetVariableOperator(
            task_id='insert_to_list_119',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": '''Industry Focus Group {{dag_run.conf.IndustryFocusGroup}} is not available in Replicon'''
            }
        )

        log_office_schedulename_120 = rail.PythonOperator(
            task_id='log_office_schedulename_120',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Default Schedule")
        )

        _adhoc_http_action_121 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_121',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        log_gettherequiredofficeschedule_uri_122 = rail.PythonOperator(
            task_id='log_gettherequiredofficeschedule_uri_122',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_121'), 'displayText', rail.result('log_office_schedulename_120'), 'uri')
        )

        if_log_gettherequiredofficeschedule_uri_122_present_123 = rail.IfOperator(
            task_id='if_log_gettherequiredofficeschedule_uri_122_present_123',
            test='''{{ result('log_gettherequiredofficeschedule_uri_122') | is_truthy }}''',
            yes_task="assign_initial_schedule_124",
            no_task="log_activitieslist_129",
        )

        assign_initial_schedule_124 = rail.RepliconServiceOperator(
            task_id='assign_initial_schedule_124',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user_43').uri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy":
                        {
                            "officeSchedule":
                                {
                                    "officeScheduleUri": "{{ result('log_gettherequiredofficeschedule_uri_122') }}"
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        }
                    }
                ]
            }
        )

        # declare_variable_detailsfor_add_user_126 = rail.SetVariableOperator(
        #     task_id='declare_variable_detailsfor_add_user_126',
        #     append=False,
        #     name='Details for Add User Job',
        #     value=None
        # )

        # if_request_type_equals_to_rehire_127 = rail.IfOperator(
        #     task_id='if_request_type_equals_to_rehire_127',
        #     test='''{{ dag_run.conf.type == 'Rehire'  or dag_run.conf.type == 'Transfer' }}''',
        #     yes_task="update_variable_detailsfor_add_user_128",
        #     no_task="log_activitieslist_129",
        # )

        def get_validation_message(dag_run):
            final_validation = ""
            validation = rail.get_dag_run_var(
                rail.result('declare_list_2')['name'])
            validations = [v['log'] for v in validation]
            if validations:
                final_validation = "New user profile added partially as" + \
                    rail.smartjoin_by_delim(validations, ',')
            else:
                final_validation = "New user profile added successfully"
            if dag_run.conf['type'] == 'Rehire' or dag_run.conf['type'] == 'Transfer':
                if validations:
                    final_validation = "New user profile for " + \
                        dag_run.conf['type'] + " user added partially as " + \
                        rail.smartjoin_by_delim(validations, ',')
                else:
                    final_validation = "New user profile for " + \
                        dag_run.conf['type'] + " user added successfully"
            return final_validation

        # update_variable_detailsfor_add_user_128 = rail.SetVariableOperator(
        #     task_id='update_variable_detailsfor_add_user_128',
        #     append=False,
        #     name='{{ result("declare_variable_detailsfor_add_user_126").name }}',
        #     value='''{{ data.workato_variable.declare_list_2.list_items[0].log') | is_truthy ? "New user profile for " + _('dag_run.conf.type.to_s + " user added partially as " + (_('data.workato_variable.declare_list_2.list_items').pluck('log').smart_join(",")) : "New user profile for " + _('dag_run.conf.type + " user added successfully '''
        # )

        def get_mapper_list(dag_run, entity_type_1, entity_type_2):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['type'] == entity_type_1
                and x['identifier_1_(_legal_entity_code/_type/_timeoff_type)'] == entity_type_2, slovakia_master_mapper))
            return [emp_type['value'] for emp_type in emp_types] if emp_types else []

        log_activitieslist_129 = rail.PythonOperator(
            task_id='log_activitieslist_129',
            python_callable=lambda dag_run: get_mapper_list(
                dag_run, "Activity", rail.result('log_valueforpayrulederivation_34'))
        )

        declare_list_131 = rail.SetVariableOperator(
            task_id='declare_list_131',
            append=False,
            name='activity list',
            value=[]
        )

        get_all_activities_132 = rail.RepliconServiceOperator(
            task_id='get_all_activities_132',
            endpoint="/services/ActivityService1.svc/GetAllActivities"
        )

        def get_activity_info():
            activity_info = []
            mapper_activity = rail.result('log_activitieslist_129')
            for activity in mapper_activity:
                activityuri = rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_activities_132'), 'displayText', activity, 'uri')
                activity_info.append({
                    "name": activity,
                    "uri": activityuri
                })
            return activity_info

        log_activities_133 = rail.PythonOperator(
            task_id='log_activities_133',
            python_callable=get_activity_info
        )

        if_declare_list_131_list_items_greater_than_0_135 = rail.IfOperator(
            task_id='if_declare_list_131_list_items_greater_than_0_135',
            test='''{{ result('log_activities_133') | length > 0 }}''',
            yes_task="put_activity_assignments_for_user_137",
            no_task="trigger_dag_run_live_ge_slovakia_child_workflow_to_add_timeoff_type_for_new_user_v1_0138",
        )

        put_activity_assignments_for_user_137 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_137',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_43')['uri'],
                "activityUris": [activity['uri'] for activity in rail.result('log_activities_133')]
            }
        )

        trigger_dag_run_live_ge_slovakia_child_workflow_to_add_timeoff_type_for_new_user_v1_0138 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_ge_slovakia_child_workflow_to_add_timeoff_type_for_new_user_v1_0138',
            retries=0,
            items=[1],
            trigger_dag_id=f'ge_slovakia_child_workflow_to_add_timeoff_type_for_new_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['OHRID'],
                "useruri": rail.result('create_user_43')['uri'],
                "legalentity": dag_run.conf['LegalEntity'],
                "startdate": dag_run.conf['LegalEntityHireDate'] if dag_run.conf['LegalEntityHireDate'] else dag_run.conf['HireEffectiveDate'],
                "jobpositiontitle": dag_run.conf['JobPositionTitle']
            }
        )

        wait_for_completion_trigger_dag_run_live_ge_slovakia_child_workflow_to_add_timeoff_type_for_new_user_v1_0138 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_ge_slovakia_child_workflow_to_add_timeoff_type_for_new_user_v1_0138',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_ge_slovakia_child_workflow_to_add_timeoff_type_for_new_user_v1_0138") }}'
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_139 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_139',
            message="na",
            severity=lambda: "Exception" if rail.get_dag_run_var(
                rail.result('declare_list_2')['name']) else "Success",
            properties=lambda dag_run: {
                "status": "Exception" if rail.get_dag_run_var(rail.result('declare_list_2')['name']) else "Success",
                "action": dag_run.conf['type'],
                "details": get_validation_message(dag_run),
                "child_job_id": get_dagrun_ecid(dag_run),
                "OHRID": dag_run.conf['OHRID'],
                "username": dag_run.conf['EmployeeFirstName'] + " " + dag_run.conf['EmployeeLastName']
            }
        )

        ey_user_import_logs_add_entry_141 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_add_entry_141',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "action": "{{ dag_run.conf.type }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> ey_user_import_logs_add_entry_141
        can_run_batch_task >> rail.Label('No') >> declare_list_2
        declare_list_2 >> log_checkifrequiredfieldsarenotthere_4 >> if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5
        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_6 >> \
            ey_user_import_logs_add_entry_141
        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 >> rail.Label(
            'No') >> slovakia_master_mapper_search_entries_8 >> if_first_id_blank_9
        if_first_id_blank_9 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_10 >> \
            ey_user_import_logs_add_entry_141
        if_first_id_blank_9 >> rail.Label(
            'No') >> log_employee_type_name_from_mapper_12 >> if_log_employee_type_name_from_mapper_12_blank_13
        if_log_employee_type_name_from_mapper_12_blank_13 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_14 >> ey_user_import_logs_add_entry_141
        if_log_employee_type_name_from_mapper_12_blank_13 >> rail.Label(
            'No') >> _adhoc_http_action_16 >> log_required_employee_type_uri_17 >> if_log_required_employee_type_uri_17_blank_18
        if_log_required_employee_type_uri_17_blank_18 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_19 >> ey_user_import_logs_add_entry_141
        if_log_required_employee_type_uri_17_blank_18 >> rail.Label(
            'No') >> log_required_department_name_21 >> if_log_37_blank_22
        if_log_37_blank_22 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_23 >> ey_user_import_logs_add_entry_141
        if_log_37_blank_22 >> rail.Label(
            'No') >> _adhoc_http_action_25 >> log_required_department_uri_26 >> if_log_required_department_uri_26_blank_27
        if_log_required_department_uri_26_blank_27 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_28 >> ey_user_import_logs_add_entry_141
        if_log_required_department_uri_26_blank_27 >> rail.Label('No') >> log_required_timesheet_period_name_30 >> \
            log_required_time_off_template_name_31 >> log_required_time_off_approval_path_name_32 >> log_required_timesheet_approval_path_name_33 >> \
            log_valueforpayrulederivation_34 >> log_required_payrule_name_35 >> log_required_holiday_calendar_name_36 >> \
            log_required_workweek_name_37 >> log_required_authentication_type_38 >> log_required_user_permission_set_39 >> \
            log_required_legal_entity_name_40 >> log_required_location_name_41 >> invoke_custom_ruby_code_42 >> create_user_43 >> \
            remove_timeoff_assignments_44 >> log_required_licenses_45 >> put_product_assignments_for_user_46 >> \
            log_required_timesheet_template_47 >> if_log_required_timesheet_template_47_blank_48
        if_log_required_timesheet_template_47_blank_48 >> rail.Label(
            'Yes') >> insert_to_list_49 >> if_request_supervisorssoid_present_57
        if_log_required_timesheet_template_47_blank_48 >> rail.Label(
            'No') >> _adhoc_http_action_51 >> log_required_timesheet_template_uri_52 >> if_log_required_timesheet_template_uri_52_present_53
        if_log_required_timesheet_template_uri_52_present_53 >> rail.Label(
            'Yes') >> assign_policy_set_to_user_54 >> if_request_supervisorssoid_present_57
        if_log_required_timesheet_template_uri_52_present_53 >> rail.Label(
            'No') >> insert_to_list_56 >> if_request_supervisorssoid_present_57
        if_request_supervisorssoid_present_57 >> rail.Label(
            'Yes') >> if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_58
        if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_58 >> rail.Label(
            'Yes') >> insert_to_list_59 >> _adhoc_http_action_82
        if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_58 >> rail.Label(
            'No') >> if_request_supervisorssoid_present_61
        if_request_supervisorssoid_present_61 >> rail.Label(
            'Yes') >> search_users_search_supervisor_62 >> if_log_supervisor_uri_63_blank_64
        if_log_supervisor_uri_63_blank_64 >> rail.Label(
            'Yes') >> ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_65 >> _adhoc_http_action_82
        if_log_supervisor_uri_63_blank_64 >> rail.Label(
            'No') >> if_downcase_not_equals_to_true_68
        if_downcase_not_equals_to_true_68 >> rail.Label(
            'Yes') >> ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_69 >> _adhoc_http_action_82
        if_downcase_not_equals_to_true_68 >> rail.Label(
            'No') >> _adhoc_http_action_search_supervisor_71 >> log_required_supervisor_permission_72 >> \
            log_supervisorpermissionassignedtouser_73 >> log_end_userpermissionassignedtouser_74 >> if_log_supervisorpermissionassignedtouser_73_blank_75
        if_log_supervisorpermissionassignedtouser_73_blank_75 >> rail.Label(
            'Yes') >> _adhoc_http_action_search_supervisor_76 >> get_permission_uris >> should_add_missing_permissions
        should_add_missing_permissions >> rail.Label(
            'No') >> assign_initital_supervisor_81
        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_supervisor_permissions >> assign_initital_supervisor_81
        if_log_supervisorpermissionassignedtouser_73_blank_75 >> rail.Label(
            'No') >> assign_initital_supervisor_81 >> _adhoc_http_action_82
        if_request_supervisorssoid_present_61 >> rail.Label(
            'No') >> _adhoc_http_action_82
        if_request_supervisorssoid_present_57 >> rail.Label(
            'No') >> _adhoc_http_action_82 >> if_request_hrmssoid_present_83
        if_request_hrmssoid_present_83 >> rail.Label(
            'Yes') >> log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84 >> if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84_blank_85
        if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84_blank_85 >> rail.Label(
            'Yes') >> insert_to_list_86 >> if_request_hrmname_present_89
        if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84_blank_85 >> rail.Label(
            'No') >> updated_u_d_ffor_h_r_m_s_s_o_i_d_88 >> if_request_hrmname_present_89
        if_request_hrmssoid_present_83 >> rail.Label(
            'No') >> if_request_hrmname_present_89
        if_request_hrmname_present_89 >> rail.Label(
            'Yes') >> log_get_u_d_f_uri_for_h_r_m_name_90 >> log_get_u_d_f_uri_for_h_r_m_name_91 >> if_log_get_u_d_f_uri_for_h_r_m_name_90_blank_92
        if_log_get_u_d_f_uri_for_h_r_m_name_90_blank_92 >> rail.Label(
            'Yes') >> insert_to_list_93 >> if_request_jobpositiontitle_present_96
        if_log_get_u_d_f_uri_for_h_r_m_name_90_blank_92 >> rail.Label(
            'No') >> updated_u_d_ffor_h_r_m_name_95 >> if_request_jobpositiontitle_present_96
        if_request_hrmname_present_89 >> rail.Label(
            'No') >> if_request_jobpositiontitle_present_96
        if_request_jobpositiontitle_present_96 >> rail.Label(
            'Yes') >> log_get_u_d_f_uri_for_job_position_title_97 >> if_log_get_u_d_f_uri_for_job_position_title_97_blank_98
        if_log_get_u_d_f_uri_for_job_position_title_97_blank_98 >> rail.Label(
            'Yes') >> insert_to_list_99 >> if_request_suspendassignmentcategory_present_102
        if_log_get_u_d_f_uri_for_job_position_title_97_blank_98 >> rail.Label(
            'No') >> updated_u_d_ffor_job_position_title_101 >> if_request_suspendassignmentcategory_present_102
        if_request_jobpositiontitle_present_96 >> rail.Label(
            'No') >> if_request_suspendassignmentcategory_present_102
        if_request_suspendassignmentcategory_present_102 >> rail.Label(
            'Yes') >> log_get_u_d_f_uri_for_suspend_assignment_category_103 >> if_log_get_u_d_f_uri_for_suspend_assignment_category_103_blank_104
        if_log_get_u_d_f_uri_for_suspend_assignment_category_103_blank_104 >> rail.Label(
            'Yes') >> insert_to_list_105 >> if_request_industryfocusgroup_present_113
        if_log_get_u_d_f_uri_for_suspend_assignment_category_103_blank_104 >> rail.Label(
            'No') >> _adhoc_http_action_107 >> log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108 >> \
            if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108_blank_109
        if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108_blank_109 >> rail.Label(
            'Yes') >> insert_to_list_110 >> if_request_industryfocusgroup_present_113
        if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108_blank_109 >> rail.Label(
            'No') >> updated_u_d_ffor_suspend_assignment_category_112 >> if_request_industryfocusgroup_present_113
        if_request_suspendassignmentcategory_present_102 >> rail.Label(
            'No') >> if_request_industryfocusgroup_present_113
        if_request_industryfocusgroup_present_113 >> rail.Label(
            'Yes') >> _adhoc_http_action_114 >> log_gettherequired_industry_focus_groupvalue_uri_115 >> \
            if_log_gettherequired_industry_focus_groupvalue_uri_115_present_116
        if_log_gettherequired_industry_focus_groupvalue_uri_115_present_116 >> rail.Label(
            'Yes') >> put_industry_focus_group_schedule_for_user_division_117 >> log_office_schedulename_120
        if_log_gettherequired_industry_focus_groupvalue_uri_115_present_116 >> rail.Label(
            'No') >> insert_to_list_119 >> log_office_schedulename_120
        if_request_industryfocusgroup_present_113 >> rail.Label(
            'No') >> log_office_schedulename_120 >> _adhoc_http_action_121 >> log_gettherequiredofficeschedule_uri_122 >> \
            if_log_gettherequiredofficeschedule_uri_122_present_123
        if_log_gettherequiredofficeschedule_uri_122_present_123 >> rail.Label(
            'No') >> log_activitieslist_129
        if_log_gettherequiredofficeschedule_uri_122_present_123 >> rail.Label(
            'Yes') >> assign_initial_schedule_124 >> log_activitieslist_129 >> declare_list_131 >> get_all_activities_132 >> \
            log_activities_133 >> if_declare_list_131_list_items_greater_than_0_135
        if_declare_list_131_list_items_greater_than_0_135 >> rail.Label(
            'Yes') >> put_activity_assignments_for_user_137 >> trigger_dag_run_live_ge_slovakia_child_workflow_to_add_timeoff_type_for_new_user_v1_0138
        if_declare_list_131_list_items_greater_than_0_135 >> rail.Label(
            'No') >> trigger_dag_run_live_ge_slovakia_child_workflow_to_add_timeoff_type_for_new_user_v1_0138 >> \
            wait_for_completion_trigger_dag_run_live_ge_slovakia_child_workflow_to_add_timeoff_type_for_new_user_v1_0138 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_139 >> \
            ey_user_import_logs_add_entry_141 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)

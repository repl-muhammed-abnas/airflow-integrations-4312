
from datetime import timedelta, datetime
import itertools
import pendulum
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
from ge.user_sync_portugal.portugal_master_mapper import portugal_master_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_portugal_add_v1_0_{config.instance}',
        description=f' GE_portugal Add V1.0 {config.instance}',
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
            no_task='declare_list_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_3',
            end_task='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_119',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='exception logger',
            value=[]
        )

        def required_field_validation(dag_run):
            validation_message = []
            if dag_run.conf['EmployeeFirstName'] is None:
                validation_message.append('Employee First  Name not present')
            if dag_run.conf['EmployeeLastName'] is None:
                validation_message.append('Employee Last  Name not present')
            if dag_run.conf['OHRID'] is None:
                validation_message.append('OHRID not present')
            if dag_run.conf['LegalEntityHireDate'] is None or dag_run.conf['HireEffectiveDate'] is None:
                validation_message.append(
                    'Legal Entity Hire Date or Hire Effective date is not present')
            if dag_run.conf['LegalEntityHireDate'] and "/" not in dag_run.conf['LegalEntityHireDate']:
                validation_message.append(
                    'Legal Entry Hire date not in allowed format')
            if dag_run.conf['HireEffectiveDate'] and "/" not in dag_run.conf['HireEffectiveDate']:
                validation_message.append(
                    'Hire Effective date not in allowed format')
            if dag_run.conf['LegalEntity'] is None:
                validation_message.append('Legal Entity not present')
            return rail.smartjoin_by_delim(validation_message, ';')

        log_checkifrequiredfieldsarenotthere_4 = rail.PythonOperator(
            task_id='log_checkifrequiredfieldsarenotthere_4',
            python_callable=required_field_validation
        )

        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 = rail.IfOperator(
            task_id='if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5',
            test='''{{ result('log_checkifrequiredfieldsarenotthere_4') | is_truthy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_6",
            no_task="ge_portugal_user_sync_master_mapper_search_entries_8",
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
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
            }
        )

        ge_portugal_user_sync_master_mapper_search_entries_8 = rail.PythonOperator(
            task_id='ge_portugal_user_sync_master_mapper_search_entries_8',
            python_callable=lambda dag_run:  list(
                filter(lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'], portugal_master_mapper))
        )

        if_first_id_blank_9 = rail.IfOperator(
            task_id='if_first_id_blank_9',
            test='''{{ result('ge_portugal_user_sync_master_mapper_search_entries_8') | is_falsy }}''',
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
                "details": 'Legal Entity is not available in Mapper',
                "child_job_id": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}",
                "OHRID": "{{ dag_run.conf.OHRID }}"
            }
        )

        def get_employe_type_from_mapper(dag_run):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Employee Type", portugal_master_mapper))
            return emp_types[0]['value'] if emp_types else None

        log_employee_type_name_from_mapper_12 = rail.PythonOperator(
            task_id='log_employee_type_name_from_mapper_12',
            python_callable=get_employe_type_from_mapper
        )

        if_log_employee_type_name_from_mapper_12_blank_13 = rail.IfOperator(
            task_id='if_log_employee_type_name_from_mapper_12_blank_13',
            test='''{{ result('log_employee_type_name_from_mapper_12') | is_falsy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_14",
            no_task="if_request_departmenturi_blank_16",
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
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
            }
        )

        def get_entity_from_mapper(dag_run, entity_type):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == entity_type, portugal_master_mapper))
            return emp_types[0]['value'] if emp_types else None

        if_request_departmenturi_blank_16 = rail.IfOperator(
            task_id='if_request_departmenturi_blank_16',
            test='''{{ dag_run.conf.DepartmentUri | is_falsy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_17",
            no_task="log_required_timesheet_period_name_19",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_17 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_17',
            message="na",
            severity="Skipped",
            properties={
                "action": "Add",
                "status": "Skipped",
                "details": '''Department is not available in Mapper for Legal Entity {{ dag_run.conf.LegalEntity }}''',
                "child_job_id": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}",
                "OHRID": "{{ dag_run.conf.OHRID }}"
            }
        )

        def get_entity_uri_from_mapper(dag_run, entity_type):
            entities = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == entity_type, portugal_master_mapper))
            return entities[0]['default__uri'] if entities else None

        log_required_timesheet_period_name_19 = rail.PythonOperator(
            task_id='log_required_timesheet_period_name_19',
            python_callable=lambda dag_run: get_entity_uri_from_mapper(
                dag_run, "Timesheet Period")
        )

        log_required_time_off_template_name_20 = rail.PythonOperator(
            task_id='log_required_time_off_template_name_20',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Timeoff Template")
        )

        log_required_time_off_approval_path_name_21 = rail.PythonOperator(
            task_id='log_required_time_off_approval_path_name_21',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Timeoff Approval Path")
        )

        def get_value_from_mapper(dag_run, entity_type1, entity_type2):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['type'] == entity_type1
                and x['identifier_1_(_legal_entity_code/_type/_timeoff_type)'] == entity_type2, portugal_master_mapper))
            return emp_types[0]['value'] if emp_types else None

        def get_holiday_calendar_name(dag_run):
            holiday_calendar_name = get_value_from_mapper(
                dag_run, "Holiday Calendar", dag_run.conf['Work'])
            return holiday_calendar_name if holiday_calendar_name else "Portugal"

        log_required_holiday_calendar_name_24 = rail.PythonOperator(
            task_id='log_required_holiday_calendar_name_24',
            python_callable=get_holiday_calendar_name
        )

        log_required_workweek_name_25 = rail.PythonOperator(
            task_id='log_required_workweek_name_25',
            python_callable=lambda dag_run: get_entity_uri_from_mapper(
                dag_run, "Work Week")
        )

        log_required_authentication_type_26 = rail.PythonOperator(
            task_id='log_required_authentication_type_26',
            python_callable=lambda dag_run: get_entity_uri_from_mapper(
                dag_run, "Authentication type"))

        log_required_user_permission_set_27 = rail.PythonOperator(
            task_id='log_required_user_permission_set_27',
            python_callable=lambda dag_run: get_value_from_mapper(
                dag_run, "Permission", "User")
        )

        log_required_legal_entity_name_28 = rail.PythonOperator(
            task_id='log_required_legal_entity_name_28',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Legal Entity")
        )

        log_required_location_name_29 = rail.PythonOperator(
            task_id='log_required_location_name_29',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Location")
        )

        log_required_schedule_name_30 = rail.PythonOperator(
            task_id='log_required_schedule_name_30',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run, "Default Schedule")
        )

        invoke_custom_ruby_code_31 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_31',
            python_callable=lambda dag_run: dag_run.conf[
                'LegalEntityHireDate'] or dag_run.conf['HireEffectiveDate']
        )

        create_user_32 = rail.RepliconServiceOperator(
            task_id='create_user_32',
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
                        "uri": dag_run.conf['DepartmentUri'],
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": "8|8|8|8|8|0|0",
                                "officeSchedule": {
                                    "officeScheduleUri": null,
                                    "name": "8|8|8|8|8|0|0"
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "workWeekStartDayUri": rail.result('log_required_workweek_name_25'),
                    "employmentDateRange": {
                        "startDate": {
                            "year": datetime.strptime(rail.result('invoke_custom_ruby_code_31'), '%d/%m/%Y').year,
                            "month": datetime.strptime(rail.result('invoke_custom_ruby_code_31'), '%d/%m/%Y').month,
                            "day": datetime.strptime(rail.result('invoke_custom_ruby_code_31'), '%d/%m/%Y').day,
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            rail.result('log_required_authentication_type_26')
                        ],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['OHRID'],
                        "SSOName": dag_run.conf['OHRID'],
                        "password": null
                    },
                    "holidayCalendar": {
                        "uri": null,
                        "name": rail.result('log_required_holiday_calendar_name_24')
                    },
                    "timeOffPolicy": null,
                    "permissionSets": [
                        {
                            "uri": null,
                            "name": rail.result('log_required_user_permission_set_27')
                        }
                    ],
                    "policySets": [
                        {
                            "uri": null,
                            "name": rail.result('log_required_time_off_template_name_20')
                        }
                    ],
                    "employeeType": {
                        "uri": null,
                        "name": rail.result('log_employee_type_name_from_mapper_12')
                    },
                    "timesheetPeriodTypeUri": rail.result('log_required_timesheet_period_name_19'),
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": {
                        "uri": null,
                        "name": rail.result('log_required_time_off_approval_path_name_21')
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
                                "name": rail.result('log_required_location_name_29')
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
                                "name": rail.result('log_required_legal_entity_name_28')
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
                    "payRuleScriptSchedule": []
                }
            }
        )

        remove_timeoff_assignments_33 = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignments_33',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_32').uri }}",
                "timeOffTypeUris": []
            }
        )

        def get_entity_uris_from_mapper(dag_run, entity_type):
            entity_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == entity_type, portugal_master_mapper))
            entity_values = [entity['default__uri'] for entity in entity_types]
            return rail.smartjoin_by_delim(entity_values, ',')

        log_required_licenses_34 = rail.PythonOperator(
            task_id='log_required_licenses_34',
            python_callable=lambda dag_run: get_entity_uris_from_mapper(
                dag_run, "License")
        )

        put_product_assignments_for_user_35 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_35',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_32')['uri'],
                "productUris": rail.result('log_required_licenses_34').split(',')
            }
        )

        log_required_language_36 = rail.PythonOperator(
            task_id='log_required_language_36',
            python_callable=lambda dag_run:  get_entity_uri_from_mapper(
                dag_run, "Language")
        )

        update_language_37 = rail.RepliconServiceOperator(
            task_id='update_language_37',
            endpoint="/services/InternationalizationService1.svc/UpdateLanguageForUser",
            data={
                "userUri": "{{ result('create_user_32').uri }}",
                "languageUri": "{{ result('log_required_language_36') }}"
            }
        )

        if_request_supervisorssoid_present_48 = rail.IfOperator(
            task_id='if_request_supervisorssoid_present_48',
            test='''{{ dag_run.conf.SupervisorSSOID | is_truthy }}''',
            yes_task="if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_49",
            no_task="_adhoc_http_action_72",
        )

        if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_49 = rail.IfOperator(
            task_id='if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_49',
            test='''{{ dag_run.conf.SupervisorSSOID == dag_run.conf.OHRID }}''',
            yes_task="insert_to_list_50",
            no_task="search_users_search_supervisor_52",
        )

        insert_to_list_50 = rail.SetVariableOperator(
            task_id='insert_to_list_50',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Supervisor not assigned since the user and supervisor SSO ID are same"
            }
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

        search_users_search_supervisor_52 = rail.RepliconServicePageOperator(
            task_id='search_users_search_supervisor_52',
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

        if_log_supervisor_uri_53_blank_54 = rail.IfOperator(
            task_id='if_log_supervisor_uri_53_blank_54',
            test='''{{ result('search_users_search_supervisor_52') | is_falsy or result('search_users_search_supervisor_52').useruri | is_falsy }}''',
            yes_task="ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_55",
            no_task="if_downcase_not_equals_to_true_58",
        )

        ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_55 = rail.WriteLogOperator(
            task_id='ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_55',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "username": dag_run.conf['OHRID'],
                "useruri": rail.result('create_user_32')['uri'],
                "supervisorloginname": dag_run.conf['SupervisorSSOID'],
                "action": "Add",
                "childjobid": get_dagrun_ecid(dag_run),
                "status": "queued",
                "supervisoreffectivedate": pendulum.now(
                    config.pacific_timezone).strftime("%d/%m/%Y"),
                "supervisorusername": dag_run.conf['SupervisorName']
            }
        )

        if_downcase_not_equals_to_true_58 = rail.IfOperator(
            task_id='if_downcase_not_equals_to_true_58',
            test='''{{ result('search_users_search_supervisor_52') | is_truthy and result('search_users_search_supervisor_52').status | lower != 'true' }}''',
            yes_task="ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_59",
            no_task="_adhoc_http_action_search_supervisor_61",
        )

        ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_59 = rail.WriteLogOperator(
            task_id='ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_59',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "username": dag_run.conf['OHRID'],
                "useruri": rail.result('create_user_32')['uri'],
                "supervisorloginname": dag_run.conf['SupervisorSSOID'],
                "action": "Add",
                "childjobid": get_dagrun_ecid(dag_run),
                "status": "queued",
                "supervisoreffectivedate": pendulum.now(
                    config.pacific_timezone).strftime("%d/%m/%Y"),
                "supervisorusername": dag_run.conf['SupervisorName']
            }
        )

        _adhoc_http_action_search_supervisor_61 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_search_supervisor_61',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_search_supervisor_52').useruri }}"
            }
        )

        def get_entity_types_from_mapper(dag_run, entity_type, identifier1):
            entity_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['type'] == entity_type
                and x['identifier_1_(_legal_entity_code/_type/_timeoff_type)'] == identifier1, portugal_master_mapper))
            entity_values = [entity['value'] for entity in entity_types]
            return rail.smartjoin_by_delim(entity_values, ';')

        def get_permission_type(permission_uri):
            permissionset = rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_search_supervisor_61'), 'policyUri', permission_uri, 'permissionSet')
            return permissionset['name'] if permissionset else None

        log_required_supervisor_permission_62 = rail.PythonOperator(
            task_id='log_required_supervisor_permission_62',
            python_callable=lambda dag_run: get_entity_types_from_mapper(
                dag_run, "Permission", "Supervsior")
        )

        log_supervisorpermissionassignedtouser_63 = rail.PythonOperator(
            task_id='log_supervisorpermissionassignedtouser_63',
            python_callable=lambda: get_permission_type(
                'urn:replicon:policy:supervision')
        )

        log_end_userpermissionassignedtouser_64 = rail.PythonOperator(
            task_id='log_end_userpermissionassignedtouser_64',
            python_callable=lambda:  get_permission_type(
                'urn:replicon:policy:user')
        )

        def is_valid_permission():
            if rail.result('log_supervisorpermissionassignedtouser_63') is None or rail.result('log_end_userpermissionassignedtouser_64') is None or \
                    rail.result('log_supervisorpermissionassignedtouser_63') not in rail.result('log_required_supervisor_permission_62') \
                    or rail.result('log_end_userpermissionassignedtouser_64') not in rail.result('log_required_supervisor_permission_62'):
                return True
            return False

        if_log_supervisorpermissionassignedtouser_63_blank_65 = rail.IfOperator(
            task_id='if_log_supervisorpermissionassignedtouser_63_blank_65',
            test=is_valid_permission,
            yes_task="_adhoc_http_action_search_supervisor_66",
            no_task="assign_initital_supervisor_71",
        )

        _adhoc_http_action_search_supervisor_66 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_search_supervisor_66',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        def get_super_user_permissions(dag_run, entity_type_1, entity_type_2):
            super_permissions = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['type'] == entity_type_1
                and x['identifier_1_(_legal_entity_code/_type/_timeoff_type)'] == entity_type_2, portugal_master_mapper))
            return [permission['value'] for permission in super_permissions] if super_permissions else []

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
            no_task='assign_initital_supervisor_71'
        )

        add_supervisor_permissions = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result('get_permission_uris'),
            execution_timeout=timedelta(days=14),
            data={
                'userUri': "{{ result('search_users_search_supervisor_52').useruri }}",
                'permissionSetUri': '{{ item }}'
            }
        )

        assign_initital_supervisor_71 = rail.RepliconServiceOperator(
            task_id='assign_initital_supervisor_71',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_32').uri }}",
                "supervisorUri": "{{ result('search_users_search_supervisor_52').useruri }}",
                "dateRange": null
            }
        )

        _adhoc_http_action_72 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_72',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'HRMSSOID': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'HRM SSO ID', 'uri', ''),
                'HRMName': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'HRM Name', 'uri', ''),
                'JobPositionTitle': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job/Position Title', 'uri', ''),
                'SuspendAssignmentCategory': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Suspend Assignment Category', 'uri', '')
            }
        )

        if_request_hrmssoid_present_73 = rail.IfOperator(
            task_id='if_request_hrmssoid_present_73',
            test='''{{ dag_run.conf.HRMSSOID | is_truthy }}''',
            yes_task="if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_74_blank_75",
            no_task="if_request_hrmname_present_79",
        )

        if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_74_blank_75 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_74_blank_75',
            test='''{{ result('_adhoc_http_action_72') | is_falsy or result('_adhoc_http_action_72').HRMSSOID | is_falsy }}''',
            yes_task="insert_to_list_76",
            no_task="updated_u_d_ffor_h_r_m_s_s_o_i_d_78",
        )

        insert_to_list_76 = rail.SetVariableOperator(
            task_id='insert_to_list_76',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "HRM SSO ID udf is not available"
            }
        )

        updated_u_d_ffor_h_r_m_s_s_o_i_d_78 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_h_r_m_s_s_o_i_d_78',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_32').uri }}",
                "customFieldUri": "{{ result('_adhoc_http_action_72').HRMSSOID }}",
                "value": "{{ dag_run.conf.HRMSSOID }}"
            }
        )

        if_request_hrmname_present_79 = rail.IfOperator(
            task_id='if_request_hrmname_present_79',
            test='''{{ dag_run.conf.HRMName | is_truthy }}''',
            yes_task="if_log_get_u_d_f_uri_for_h_r_m_name_80_blank_81",
            no_task="if_request_jobpositiontitle_present_85",
        )

        if_log_get_u_d_f_uri_for_h_r_m_name_80_blank_81 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_h_r_m_name_80_blank_81',
            test='''{{ result('_adhoc_http_action_72') | is_falsy or result('_adhoc_http_action_72').HRMName | is_falsy }}''',
            yes_task="insert_to_list_82",
            no_task="updated_u_d_ffor_h_r_m_name_84",
        )

        insert_to_list_82 = rail.SetVariableOperator(
            task_id='insert_to_list_82',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "HRM Name udf is not available"
            }
        )

        updated_u_d_ffor_h_r_m_name_84 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_h_r_m_name_84',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_32').uri }}",
                "customFieldUri": "{{ result('_adhoc_http_action_72').HRMName }}",
                "value": "{{ dag_run.conf.HRMName }}"
            }
        )

        if_request_jobpositiontitle_present_85 = rail.IfOperator(
            task_id='if_request_jobpositiontitle_present_85',
            test='''{{ dag_run.conf.JobPositionTitle | is_truthy }}''',
            yes_task="if_log_get_u_d_f_uri_for_job_position_title_86_blank_87",
            no_task="if_request_suspendassignmentcategory_present_91",
        )

        if_log_get_u_d_f_uri_for_job_position_title_86_blank_87 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_job_position_title_86_blank_87',
            test='''{{ result('_adhoc_http_action_72') | is_falsy or result('_adhoc_http_action_72').JobPositionTitle | is_falsy }}''',
            yes_task="insert_to_list_88",
            no_task="updated_u_d_ffor_job_position_title_90",
        )

        insert_to_list_88 = rail.SetVariableOperator(
            task_id='insert_to_list_88',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Job/Position Title udf is not available"
            }
        )

        updated_u_d_ffor_job_position_title_90 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_job_position_title_90',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_32').uri }}",
                "customFieldUri": "{{ result('_adhoc_http_action_72').JobPositionTitle }}",
                "value": "{{ dag_run.conf.JobPositionTitle }}"
            }
        )

        if_request_suspendassignmentcategory_present_91 = rail.IfOperator(
            task_id='if_request_suspendassignmentcategory_present_91',
            test='''{{ dag_run.conf.SuspendAssignmentCategory | is_truthy }}''',
            yes_task="if_log_get_u_d_f_uri_for_suspend_assignment_category_92_blank_93",
            no_task="if_request_industryfocusgroup_present_102",
        )

        if_log_get_u_d_f_uri_for_suspend_assignment_category_92_blank_93 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_suspend_assignment_category_92_blank_93',
            test='''{{ result('_adhoc_http_action_72') | is_falsy or result('_adhoc_http_action_72').SuspendAssignmentCategory | is_falsy }}''',
            yes_task="insert_to_list_94",
            no_task="_adhoc_http_action_96",
        )

        insert_to_list_94 = rail.SetVariableOperator(
            task_id='insert_to_list_94',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Suspend Assignment Category udf is not available"
            }
        )

        _adhoc_http_action_96 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_96',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('_adhoc_http_action_72').SuspendAssignmentCategory }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['SuspendAssignmentCategory'], 'uri', '')
        )

        if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_97_blank_98 = rail.IfOperator(
            task_id='if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_97_blank_98',
            test='''{{ result('_adhoc_http_action_96') | is_falsy }}''',
            yes_task="insert_to_list_99",
            no_task="updated_u_d_ffor_suspend_assignment_category_101",
        )

        insert_to_list_99 = rail.SetVariableOperator(
            task_id='insert_to_list_99',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": '''Suspend Assignment Category value "{{dag_run.conf.AssignmentCategory}}" is not available in Replicon'''
            }
        )

        updated_u_d_ffor_suspend_assignment_category_101 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_suspend_assignment_category_101',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user_32').uri }}",
                "customFieldUri": "{{ result('_adhoc_http_action_72').SuspendAssignmentCategory }}",
                "customFieldDropDownOptionUri": "{{ result('_adhoc_http_action_96') }}"
            }
        )

        if_request_industryfocusgroup_present_102 = rail.IfOperator(
            task_id='if_request_industryfocusgroup_present_102',
            test='''{{ dag_run.conf.IndustryFocusGroup | is_truthy }}''',
            yes_task="_adhoc_http_action_103",
            no_task="trigger_dag_run_live_ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0109",
        )

        _adhoc_http_action_103 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_103',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
        )

        log_gettherequired_industry_focus_groupvalue_uri_104 = rail.PythonOperator(
            task_id='log_gettherequired_industry_focus_groupvalue_uri_104',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_103'), 'displayText', dag_run.conf['IndustryFocusGroup'], 'uri') if rail.result('_adhoc_http_action_103') else None
        )

        if_log_gettherequired_industry_focus_groupvalue_uri_104_present_105 = rail.IfOperator(
            task_id='if_log_gettherequired_industry_focus_groupvalue_uri_104_present_105',
            test='''{{ result('log_gettherequired_industry_focus_groupvalue_uri_104') | is_truthy }}''',
            yes_task="put_industry_focus_group_schedule_for_user_division_106",
            no_task="insert_to_list_108",
        )

        put_industry_focus_group_schedule_for_user_division_106 = rail.RepliconServiceOperator(
            task_id='put_industry_focus_group_schedule_for_user_division_106',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data={
                "userUri": "{{ result('create_user_32').uri }}",
                "scheduleEntries": [
                    {
                        "division": {
                            "uri": "{{ result('log_gettherequired_industry_focus_groupvalue_uri_104') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        insert_to_list_108 = rail.SetVariableOperator(
            task_id='insert_to_list_108',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": '''Industry Focus Group "{{dag_run.conf.IndustryFocusGroup}}" is not available in Replicon'''
            }
        )

        trigger_dag_run_live_ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0109 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0109',
            retries=0,
            items=[1],
            trigger_dag_id=f'ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "userloginname": "{{ dag_run.conf.OHRID }}",
                "useruri": "{{ result('create_user_32').uri }}",
                "legalentity": "{{ dag_run.conf.LegalEntity }}",
                "startdate": "{{ result('invoke_custom_ruby_code_31') }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "type": "Add"
            }
        )

        wait_for_completion_trigger_dag_run_live_ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0109 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0109',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0109") }}'
        )

        if_reply_output_present_110 = rail.IfOperator(
            # Todo
            task_id='if_reply_output_present_110',
            test='''{{ result('trigger_dag_run_live_ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0109') | is_truthy }}''',
            yes_task="insert_to_list_111",
            no_task="declare_variable_detailsfor_add_user_112",
        )

        insert_to_list_111 = rail.SetVariableOperator(
            task_id='insert_to_list_111',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "{{ result('trigger_dag_run_live_ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0109') }}"
            }
        )

        declare_variable_detailsfor_add_user_112 = rail.SetVariableOperator(
            task_id='declare_variable_detailsfor_add_user_112',
            append=False,
            name='Details for Add User Job',
            value=None
        )

        if_request_type_equals_to_add_113 = rail.IfOperator(
            task_id='if_request_type_equals_to_add_113',
            test='''{{ dag_run.conf.type == 'Add' }}''',
            yes_task="update_variable_detailsfor_add_user_114",
            no_task="if_request_type_equals_to_rehire_115",
        )

        def get_user_add_logs(user_exception_message, user_success_message):
            user_detail_log = rail.get_dag_run_var(
                rail.result("declare_list_3")['name'])
            if user_detail_log:
                validations = [v['log'] for v in user_detail_log]
                return user_exception_message + rail.smartjoin_by_delim(validations, ',')
            return user_success_message

        update_variable_detailsfor_add_user_114 = rail.SetVariableOperator(
            task_id='update_variable_detailsfor_add_user_114',
            append=False,
            name='{{ result("declare_variable_detailsfor_add_user_112").name }}',
            value=lambda: get_user_add_logs(
                'User (New) partially created, ', 'User (New) successfully created')
        )

        if_request_type_equals_to_rehire_115 = rail.IfOperator(
            task_id='if_request_type_equals_to_rehire_115',
            test='''{{ dag_run.conf.type == 'Rehire' }}''',
            yes_task="update_variable_detailsfor_add_user_116",
            no_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_117",
        )

        update_variable_detailsfor_add_user_116 = rail.SetVariableOperator(
            task_id='update_variable_detailsfor_add_user_116',
            append=False,
            name='{{ result("declare_variable_detailsfor_add_user_112").name }}',
            value=lambda: get_user_add_logs(
                'User (Rehire) partially created,', 'User (Rehire) successfully created')
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_117 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_117',
            message="na",
            severity=lambda: "Exception" if rail.get_dag_run_var(
                rail.result("declare_list_3")['name']) else "Success",
            properties=lambda dag_run: {
                "action": dag_run.conf['type'],
                "status": "Exception" if rail.get_dag_run_var(rail.result("declare_list_3")['name']) else "Success",
                "child_job_id": get_dagrun_ecid(dag_run),
                "username": dag_run.conf['EmployeeFirstName'] + " " + dag_run.conf['EmployeeLastName'],
                "details": rail.get_dag_run_var(
                    rail.result('declare_variable_detailsfor_add_user_112')['name']),
                "OHRID": "{{ dag_run.conf.OHRID }}"
            }
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_119 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_119',
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "action": "{{ dag_run.conf.type }}",
                "status": "Error",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}",
                "details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_119
        can_run_batch_task >> rail.Label('No') >> declare_list_3
        declare_list_3 >> log_checkifrequiredfieldsarenotthere_4 >> if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5
        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_6 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_119
        ge_portugal_user_sync_master_mapper_search_entries_8
        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 >> rail.Label(
            'No') >> ge_portugal_user_sync_master_mapper_search_entries_8 >> if_first_id_blank_9
        if_first_id_blank_9 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_10 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_119
        log_employee_type_name_from_mapper_12
        if_first_id_blank_9 >> rail.Label(
            'No') >> log_employee_type_name_from_mapper_12 >> if_log_employee_type_name_from_mapper_12_blank_13
        if_log_employee_type_name_from_mapper_12_blank_13 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_14 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_119
        if_request_departmenturi_blank_16
        if_log_employee_type_name_from_mapper_12_blank_13 >> rail.Label(
            'No') >> if_request_departmenturi_blank_16
        if_request_departmenturi_blank_16 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_17 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_119
        log_required_timesheet_period_name_19
        if_request_departmenturi_blank_16 >> rail.Label('No') >> log_required_timesheet_period_name_19 >> \
            log_required_time_off_template_name_20 >> log_required_time_off_approval_path_name_21 >> \
            log_required_holiday_calendar_name_24 >> log_required_workweek_name_25 >> \
            log_required_authentication_type_26 >> log_required_user_permission_set_27 >> log_required_legal_entity_name_28 >> \
            log_required_location_name_29 >> log_required_schedule_name_30 >> invoke_custom_ruby_code_31 >> \
            create_user_32 >> remove_timeoff_assignments_33 >> log_required_licenses_34 >> \
            put_product_assignments_for_user_35 >> log_required_language_36 >> update_language_37 >> \
            if_request_supervisorssoid_present_48
        if_request_supervisorssoid_present_48 >> rail.Label(
            'Yes') >> if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_49
        if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_49 >> rail.Label(
            'Yes') >> insert_to_list_50 >> _adhoc_http_action_72
        if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_49 >> rail.Label(
            'No') >> search_users_search_supervisor_52 >> if_log_supervisor_uri_53_blank_54
        if_log_supervisor_uri_53_blank_54 >> rail.Label(
            'Yes') >> ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_55 >> _adhoc_http_action_72
        if_log_supervisor_uri_53_blank_54 >> rail.Label(
            'No') >> if_downcase_not_equals_to_true_58
        if_downcase_not_equals_to_true_58 >> rail.Label(
            'Yes') >> ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_59 >> _adhoc_http_action_72
        if_downcase_not_equals_to_true_58 >> rail.Label(
            'No') >> _adhoc_http_action_search_supervisor_61 >> log_required_supervisor_permission_62 >> \
            log_supervisorpermissionassignedtouser_63 >> log_end_userpermissionassignedtouser_64 >> if_log_supervisorpermissionassignedtouser_63_blank_65
        if_log_supervisorpermissionassignedtouser_63_blank_65 >> rail.Label(
            'Yes') >> _adhoc_http_action_search_supervisor_66 >> get_permission_uris >> \
            should_add_missing_permissions
        should_add_missing_permissions >> rail.Label(
            'No') >> assign_initital_supervisor_71
        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_supervisor_permissions >> assign_initital_supervisor_71
        if_log_supervisorpermissionassignedtouser_63_blank_65 >> rail.Label(
            'No') >> assign_initital_supervisor_71 >> _adhoc_http_action_72
        if_request_supervisorssoid_present_48 >> rail.Label(
            'No') >> _adhoc_http_action_72 >> if_request_hrmssoid_present_73
        if_request_hrmssoid_present_73 >> rail.Label(
            'Yes') >> if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_74_blank_75
        if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_74_blank_75 >> rail.Label(
            'Yes') >> insert_to_list_76 >> if_request_hrmname_present_79
        if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_74_blank_75 >> rail.Label(
            'No') >> updated_u_d_ffor_h_r_m_s_s_o_i_d_78 >> if_request_hrmname_present_79
        if_request_hrmssoid_present_73 >> rail.Label(
            'No') >> if_request_hrmname_present_79
        if_request_hrmname_present_79 >> rail.Label(
            'Yes') >> if_log_get_u_d_f_uri_for_h_r_m_name_80_blank_81
        if_log_get_u_d_f_uri_for_h_r_m_name_80_blank_81 >> rail.Label(
            'Yes') >> insert_to_list_82 >> if_request_jobpositiontitle_present_85
        if_log_get_u_d_f_uri_for_h_r_m_name_80_blank_81 >> rail.Label(
            'No') >> updated_u_d_ffor_h_r_m_name_84 >> if_request_jobpositiontitle_present_85
        if_request_hrmname_present_79 >> rail.Label(
            'No') >> if_request_jobpositiontitle_present_85
        if_request_jobpositiontitle_present_85 >> rail.Label(
            'Yes') >> if_log_get_u_d_f_uri_for_job_position_title_86_blank_87
        if_log_get_u_d_f_uri_for_job_position_title_86_blank_87 >> rail.Label(
            'Yes') >> insert_to_list_88 >> if_request_suspendassignmentcategory_present_91
        if_log_get_u_d_f_uri_for_job_position_title_86_blank_87 >> rail.Label(
            'No') >> updated_u_d_ffor_job_position_title_90 >> if_request_suspendassignmentcategory_present_91
        if_request_jobpositiontitle_present_85 >> rail.Label(
            'No') >> if_request_suspendassignmentcategory_present_91
        if_request_suspendassignmentcategory_present_91 >> rail.Label(
            'Yes') >> if_log_get_u_d_f_uri_for_suspend_assignment_category_92_blank_93
        if_log_get_u_d_f_uri_for_suspend_assignment_category_92_blank_93 >> rail.Label(
            'Yes') >> insert_to_list_94 >> if_request_industryfocusgroup_present_102
        if_log_get_u_d_f_uri_for_suspend_assignment_category_92_blank_93 >> rail.Label(
            'No') >> _adhoc_http_action_96 >> if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_97_blank_98
        if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_97_blank_98 >> rail.Label(
            'Yes') >> insert_to_list_99 >> if_request_industryfocusgroup_present_102
        if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_97_blank_98 >> rail.Label(
            'No') >> updated_u_d_ffor_suspend_assignment_category_101 >> if_request_industryfocusgroup_present_102
        if_request_suspendassignmentcategory_present_91 >> rail.Label(
            'No') >> if_request_industryfocusgroup_present_102
        if_request_industryfocusgroup_present_102 >> rail.Label(
            'Yes') >> _adhoc_http_action_103 >> log_gettherequired_industry_focus_groupvalue_uri_104 >> \
            if_log_gettherequired_industry_focus_groupvalue_uri_104_present_105
        if_log_gettherequired_industry_focus_groupvalue_uri_104_present_105 >> rail.Label(
            'Yes') >> put_industry_focus_group_schedule_for_user_division_106 >> \
            trigger_dag_run_live_ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0109
        if_log_gettherequired_industry_focus_groupvalue_uri_104_present_105 >> rail.Label(
            'No') >> insert_to_list_108 >> trigger_dag_run_live_ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0109
        if_request_industryfocusgroup_present_102 >> rail.Label(
            'No') >> trigger_dag_run_live_ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0109 >> \
            wait_for_completion_trigger_dag_run_live_ge_portugal_child_workflow_to_add_timeoff_type_for_new_user_v1_0109 >> if_reply_output_present_110
        if_reply_output_present_110 >> rail.Label(
            'Yes') >> insert_to_list_111 >> declare_variable_detailsfor_add_user_112
        if_reply_output_present_110 >> rail.Label(
            'No') >> declare_variable_detailsfor_add_user_112 >> if_request_type_equals_to_add_113
        if_request_type_equals_to_add_113 >> rail.Label(
            'Yes') >> update_variable_detailsfor_add_user_114 >> if_request_type_equals_to_rehire_115
        if_request_type_equals_to_add_113 >> rail.Label(
            'No') >> if_request_type_equals_to_rehire_115
        if_request_type_equals_to_rehire_115 >> rail.Label(
            'Yes') >> update_variable_detailsfor_add_user_116 >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_117
        if_request_type_equals_to_rehire_115 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_117 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_119 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)

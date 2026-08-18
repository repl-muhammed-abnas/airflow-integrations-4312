
from datetime import timedelta, datetime
import itertools
import pendulum
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
from ge.user_sync_netherlands.netherlands_master_mapper import netherlands_master_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_netherlands_add_v1_0_{config.instance}',
        description=f'GE netherlands Add V1.0 {config.instance}',
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
            if dag_run.conf['LegacyPayrollID'] is None:
                validation_message.append('Legacy Payroll ID not present')
            if dag_run.conf['SupervisorSSOID'] is None:
                validation_message.append('Supervisor SSO ID ID not present')
            if dag_run.conf['SupervisorName'] is None:
                validation_message.append('Supervisor Name not present')
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
            no_task="netherlands_master_mapper_search_entries_8",
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

        netherlands_master_mapper_search_entries_8 = rail.PythonOperator(
            task_id='netherlands_master_mapper_search_entries_8',
            python_callable=lambda dag_run: list(
                filter(lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'], netherlands_master_mapper))
        )

        if_first_id_blank_9 = rail.IfOperator(
            task_id='if_first_id_blank_9',
            test='''{{ result('netherlands_master_mapper_search_entries_8') | is_falsy }}''',
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
            emp_types = list(filter(lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['legacy_payroll_id']
                             == dag_run.conf['LegacyPayrollID'] and x['type'] == "Employee Type", netherlands_master_mapper))
            return emp_types[0]['value'] if emp_types else {}

        log_employee_type_name_from_mapper_12 = rail.PythonOperator(
            task_id='log_employee_type_name_from_mapper_12',
            python_callable=get_employe_type_from_mapper
        )

        def get_mapper_entry(dag_run):
            timesheettemplate = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Timesheet Template", netherlands_master_mapper))
            employeetype = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Employee Type", netherlands_master_mapper))
            timeofftemplate = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Timeoff Template", netherlands_master_mapper))
            timesheetapprovalpath = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Timesheet Approval Path", netherlands_master_mapper))
            timeoffapprovalpath = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Timeoff Approval Path", netherlands_master_mapper))
            payrulename = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Payrule", netherlands_master_mapper))
            holidaycalendar = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Holiday Calendar", netherlands_master_mapper))
            workweek = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Work Week", netherlands_master_mapper))
            authenticationtype = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Authentication type", netherlands_master_mapper))
            permissionsets = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Permission" and x['supervisor'] == "User", netherlands_master_mapper))
            legalentityname = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "legalcombination", netherlands_master_mapper))
            location = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Location", netherlands_master_mapper))
            licences = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "License", netherlands_master_mapper))
            licenses_mapper = [license['default_uri']
                               for license in licences]
            language = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Language", netherlands_master_mapper))
            start_date = dag_run.conf['LegalEntityHireDate'] or dag_run.conf['HireEffectiveDate']
            timesheetperiod = list(filter(
                lambda x: x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Timesheet Period", netherlands_master_mapper))

            return {
                "timesheettemplate": timesheettemplate[0]['value'] if timesheettemplate else None,
                "timeofftemplate": timeofftemplate[0]['value'] if timeofftemplate else None,
                "timeoffapprovalpath": timeoffapprovalpath[0]['value'] if timeoffapprovalpath else None,
                "timesheetapprovalpath": timesheetapprovalpath[0]['value'] if timesheetapprovalpath else None,
                "timesheetperiod": timesheetperiod[0]['default_uri'] if timesheetperiod else None,
                "employeetype": employeetype[0]['value'] if employeetype else None,
                "payrulename": payrulename[0]['value'] if payrulename else None,
                "holidaycalendar": holidaycalendar[0]['value'] if holidaycalendar else None,
                "workweek": workweek[0]['default_uri'] if workweek else None,
                "authenticationtype": authenticationtype[0]['default_uri'] if authenticationtype else None,
                "permissionsets": permissionsets[0]['value'] if permissionsets else None,
                "legalentityname": legalentityname[0]['value'] if legalentityname else None,
                "location": location[0]['value'] if location else None,
                "licence": licenses_mapper,
                "language": language[0]['default_uri'] if language else None,
                "startdate_day": datetime.strptime(start_date, '%d/%m/%Y').day,
                "startdate_month": datetime.strptime(start_date, '%d/%m/%Y').month,
                "startdate_year": datetime.strptime(start_date, '%d/%m/%Y').year,
                "timeofftypes": None
            }

        log_mapper_entry = rail.PythonOperator(
            task_id='log_mapper_entry',
            python_callable=get_mapper_entry
        )

        if_log_employee_type_name_from_mapper_12_blank_13 = rail.IfOperator(
            task_id='if_log_employee_type_name_from_mapper_12_blank_13',
            test='''{{ result('log_mapper_entry').employeetype | is_falsy }}''',
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
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        if_request_departmenturi_blank_16 = rail.IfOperator(
            task_id='if_request_departmenturi_blank_16',
            test='''{{ dag_run.conf.Departmenturi | is_falsy }}''',
            yes_task="ey_user_import_logs_add_entry_17",
            no_task="create_user_30",
        )

        ey_user_import_logs_add_entry_17 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_add_entry_17',
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

        def get_entity_from_mapper(dag_run, entity_type):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID']
                and x['type'] == entity_type, netherlands_master_mapper))
            return emp_types[0]['value'] if emp_types else {}

        create_user_30 = rail.RepliconServiceOperator(
            task_id='create_user_30',
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
                        "uri": dag_run.conf['Departmenturi'],
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": rail.result('log_mapper_entry')['workweek'],
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('log_mapper_entry')['startdate_year'],
                            "month": rail.result('log_mapper_entry')['startdate_month'],
                            "day": rail.result('log_mapper_entry')['startdate_day'],
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            rail.result('log_mapper_entry')[
                                'authenticationtype']
                        ],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['OHRID'],
                        "SSOName": dag_run.conf['OHRID'],
                        "password": null
                    },
                    "holidayCalendar": {
                        "uri": null,
                        "name": rail.result('log_mapper_entry')['holidaycalendar']
                    },
                    "timeOffPolicy": null,
                    "permissionSets": [
                        {
                            "uri": null,
                            "name": rail.result('log_mapper_entry')['permissionsets']
                        }
                    ],
                    "policySets": [
                        {
                            "uri": null,
                            "name": rail.result('log_mapper_entry')['timeofftemplate']
                        }
                    ],
                    "employeeType": {
                        "uri": null,
                        "name": rail.result('log_mapper_entry')['employeetype']
                    },
                    "timesheetPeriodTypeUri": rail.result('log_mapper_entry')['timesheetperiod'],
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {
                        "uri": null,
                        "name": rail.result('log_mapper_entry')['timesheetapprovalpath']
                    },
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": {
                        "uri": null,
                        "name": rail.result('log_mapper_entry')['timeoffapprovalpath']
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
                                "name": rail.result('log_mapper_entry')['location']
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
                                "name": rail.result('log_mapper_entry')['legalentityname']
                            },
                            "effectiveDate": null
                        }
                    ],
                    "serviceCenterSchedule": [
                        {
                            "serviceCenter": {
                                "uri": null,
                                "parentUri": null,
                                "name": dag_run.conf['LegacyPayrollID']
                            },
                            "effectiveDate": null
                        }
                    ],
                    "departmentGroupSchedule": [],
                    "employeeTypeGroupSchedule": [],
                    "timesheetPeriodSchedule": [],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        remove_timeoff_assignments_44 = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignments_44',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_30').uri }}",
                "timeOffTypeUris": []
            }
        )

        put_product_assignments_for_user_46 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_46',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_30')['uri'],
                "productUris": rail.result('log_mapper_entry')['licence']
            }
        )

        update_language_39 = rail.RepliconServiceOperator(
            task_id='update_language_39',
            endpoint="/services/InternationalizationService1.svc/UpdateLanguageForUser",
            data=lambda: {
                "userUri": rail.result('create_user_30')['uri'],
                "languageUri": rail.result('log_mapper_entry')['language']
            }
        )

        if_request_overtimeeligibility_contains_yes_36 = rail.IfOperator(
            task_id='if_request_overtimeeligibility_contains_yes_36',
            test='''{{ dag_run.conf.OvertimeEligibility | matches('Yes') }}''',
            yes_task="trigger_dag_run_ge_netherlands_payrule_assignment_add_update_v1_038",
            no_task="if_request_supervisorssoid_present_57",
        )

        trigger_dag_run_ge_netherlands_payrule_assignment_add_update_v1_038 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_netherlands_payrule_assignment_add_update_v1_038',
            retries=0,
            items=[-1],
            trigger_dag_id=f'ge_user_sync_netherlands_payrule_assignment_add_update_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "useruri": "{{ result('create_user_30').uri }}",
                "EmployeeFirstName": "{{ dag_run.conf.EmployeeFirstName }}",
                "EmployeeLastName": "{{ dag_run.conf.EmployeeLastName }}",
                "EmployeeEmailAddress": "{{ dag_run.conf.EmployeeEmailAddress }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "LegalEntityHireDate": "{{ dag_run.conf.LegalEntityHireDate }}",
                "LegacyPayrollID": "{{ dag_run.conf.LegacyPayrollID }}",
                "JobPositionTitle": "{{ dag_run.conf.JobPositionTitle }}",
                "SupervisorSSOID": "{{ dag_run.conf.SupervisorSSOID }}",
                "SupervisorName": "{{ dag_run.conf.SupervisorName }}",
                "LegalEntity": "{{ dag_run.conf.LegalEntity }}",
                "OvertimeEligibility": "{{ dag_run.conf.OvertimeEligibility }}",
                "HealthcareProductLineEIT": "{{ dag_run.conf.HealthcareProductLineEIT }}",
                "jobtype": "{{ dag_run.conf.JobType }}",
                "AssignmentEffectiveDate": "{{ dag_run.conf.AssignmentEffectiveDate }}",
                "type": "timesheet and payrule",
                "action": "add",
                "currenttimesheettemplate": """,
                "currentpayrulename": """,
                "userstartdate": "{{ result('log_mapper_entry').startdate_day }}/{{ result('log_mapper_entry').startdate_month }}/{{ result('log_mapper_entry').startdate_year }}"
            }
        )

        wait_for_completion_trigger_dag_run_ge_netherlands_payrule_assignment_add_update_v1_038 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_netherlands_payrule_assignment_add_update_v1_038',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_netherlands_payrule_assignment_add_update_v1_038") }}'
        )

        if_request_supervisorssoid_present_57 = rail.IfOperator(
            task_id='if_request_supervisorssoid_present_57',
            test='''{{ dag_run.conf.SupervisorSSOID | is_truthy }}''',
            yes_task="if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_58",
            no_task="_adhoc_http_action_66",
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
            no_task="_adhoc_http_action_66",
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
                "useruri": rail.result('create_user_30')['uri'],
                "supervisorloginname": dag_run.conf['SupervisorSSOID'],
                "action": "Add",
                "childjobid": get_dagrun_ecid(dag_run),
                "supervisoreffectivedate": pendulum.now(
                    config.pacific_timezone).strftime("%m/%d/%Y"),
                "status": "queued",
                "supervisorusername": dag_run.conf['SupervisorName']
            }
        )

        if_downcase_not_equals_to_true_68 = rail.IfOperator(
            task_id='if_downcase_not_equals_to_true_68',
            test='''{{ result('search_users_search_supervisor_62') | is_truthy and result('search_users_search_supervisor_62').status != 'True' }}''',
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
                "useruri": rail.result('create_user_30')['uri'],
                "supervisorloginname": dag_run.conf['SupervisorSSOID'],
                "action": "Add",
                "childjobid": get_dagrun_ecid(dag_run),
                "supervisoreffectivedate": pendulum.now(
                    config.pacific_timezone).strftime("%m/%d/%Y"),
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
                and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID']
                and x['type'] == entity_type_1
                and x['supervisor'] == entity_type_2, netherlands_master_mapper))
            return [emp_type['value'] for emp_type in emp_types] if emp_types else []

        log_required_supervisor_permission_72 = rail.PythonOperator(
            task_id='log_required_supervisor_permission_72',
            python_callable=lambda dag_run: get_super_user_permissions(
                dag_run, 'Permission', 'Supervisor')
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
                dag_run, 'Permission', 'Supervisor')
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

        assign_initital_supervisor_81 = rail.RepliconServiceOperator(
            task_id='assign_initital_supervisor_81',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_30').uri }}",
                "supervisorUri": "{{ result('search_users_search_supervisor_62').useruri }}",
                "dateRange": null
            }
        )

        _adhoc_http_action_66 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_66',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        if_request_hrmssoid_present_67 = rail.IfOperator(
            task_id='if_request_hrmssoid_present_67',
            test='''{{ dag_run.conf.HRMSSOID | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84",
            no_task="if_request_hrmname_present_89",
        )

        log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_66'), 'displayText', 'HRM SSO ID', 'uri')
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
                "objectUri": "{{ result('create_user_30').uri }}",
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
                '_adhoc_http_action_66'), 'displayText', 'HRM Name', 'uri')
        )

        log_get_u_d_f_uri_for_h_r_m_name_91 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_h_r_m_name_91',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_66'), 'displayText', 'HRM Name', 'uri')
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
                "objectUri": "{{ result('create_user_30').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_h_r_m_name_90') }}",
                "value": "{{ dag_run.conf.HRMName }}"
            }
        )

        if_request_jobpositiontitle_present_96 = rail.IfOperator(
            task_id='if_request_jobpositiontitle_present_96',
            test='''{{ dag_run.conf.JobPositionTitle | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_job_position_title_97",
            no_task="if_request_payroll_present_86",
        )

        log_get_u_d_f_uri_for_job_position_title_97 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_job_position_title_97',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_66'), 'displayText', 'Job/Position Title', 'uri')
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
                "objectUri": "{{ result('create_user_30').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_job_position_title_97') }}",
                "value": "{{ dag_run.conf.JobPositionTitle }}"
            }
        )

        if_request_payroll_present_86 = rail.IfOperator(
            task_id='if_request_payroll_present_86',
            test='''{{ dag_run.conf.Payroll | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_payroll_87",
            no_task="if_request_healthcareproductlineeit_present_92",
        )

        log_get_u_d_f_uri_for_payroll_87 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_payroll_87',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_66'), 'displayText', 'Payroll', 'uri')
        )

        if_log_get_u_d_f_uri_for_payroll_87_blank_88 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_payroll_87_blank_88',
            test='''{{ result('log_get_u_d_f_uri_for_payroll_87') | is_falsy }}''',
            yes_task="insert_to_list_89",
            no_task="updated_u_d_ffor_payroll_91",
        )

        insert_to_list_89 = rail.SetVariableOperator(
            task_id='insert_to_list_89',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Payroll udf is not available"
            }
        )

        updated_u_d_ffor_payroll_91 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_payroll_91',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_30').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_payroll_87') }}",
                "value": "{{ dag_run.conf.Payroll }}"
            }
        )

        if_request_healthcareproductlineeit_present_92 = rail.IfOperator(
            task_id='if_request_healthcareproductlineeit_present_92',
            test='''{{ dag_run.conf.HealthcareProductLineEIT | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_healthcare_product_lineeit_93",
            no_task="if_request_jobtype_present_98",
        )

        log_get_u_d_f_uri_for_healthcare_product_lineeit_93 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_healthcare_product_lineeit_93',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_66'), 'displayText', 'Healthcare Product Line EIT', 'uri')
        )

        if_log_get_u_d_f_uri_for_healthcare_product_lineeit_93_blank_94 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_healthcare_product_lineeit_93_blank_94',
            test='''{{ result('log_get_u_d_f_uri_for_healthcare_product_lineeit_93') | is_falsy }}''',
            yes_task="insert_to_list_95",
            no_task="updated_u_d_ffor_healthcare_product_line_e_i_t_97",
        )

        insert_to_list_95 = rail.SetVariableOperator(
            task_id='insert_to_list_95',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Healthcare Product Line EIT udf is not available"
            }
        )

        updated_u_d_ffor_healthcare_product_line_e_i_t_97 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_healthcare_product_line_e_i_t_97',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_30').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_healthcare_product_lineeit_93') }}",
                "value": "{{ dag_run.conf.HealthcareProductLineEIT }}"
            }
        )

        if_request_jobtype_present_98 = rail.IfOperator(
            task_id='if_request_jobtype_present_98',
            test='''{{ dag_run.conf.JobType | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_job_type_99",
            no_task="if_request_careerband_present_104",
        )

        log_get_u_d_f_uri_for_job_type_99 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_job_type_99',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_66'), 'displayText', 'Job Type', 'uri')
        )

        if_log_get_u_d_f_uri_for_job_type_99_blank_100 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_job_type_99_blank_100',
            test='''{{ result('log_get_u_d_f_uri_for_job_type_99') | is_falsy }}''',
            yes_task="insert_to_list_101",
            no_task="updated_u_d_ffor_job_type_103",
        )

        insert_to_list_101 = rail.SetVariableOperator(
            task_id='insert_to_list_101',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Job Type udf is not available"
            }
        )

        updated_u_d_ffor_job_type_103 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_job_type_103',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_30').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_job_type_99') }}",
                "value": "{{ dag_run.conf.JobType }}"
            }
        )

        if_request_careerband_present_104 = rail.IfOperator(
            task_id='if_request_careerband_present_104',
            test='''{{ dag_run.conf.CareerBand | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_career_band_105",
            no_task="if_request_work_present_110",
        )

        log_get_u_d_f_uri_for_career_band_105 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_career_band_105',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_66'), 'displayText', 'Career Band', 'uri')
        )

        if_log_get_u_d_f_uri_for_career_band_105_blank_106 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_career_band_105_blank_106',
            test='''{{ result('log_get_u_d_f_uri_for_career_band_105') | is_falsy }}''',
            yes_task="insert_to_list_107",
            no_task="updated_u_d_ffor_career_band_109",
        )

        insert_to_list_107 = rail.SetVariableOperator(
            task_id='insert_to_list_107',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Career Band udf is not available"
            }
        )

        updated_u_d_ffor_career_band_109 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_career_band_109',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_30').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_career_band_105') }}",
                "value": "{{ dag_run.conf.CareerBand }}"
            }
        )

        if_request_work_present_110 = rail.IfOperator(
            task_id='if_request_work_present_110',
            test='''{{ dag_run.conf.WorkLocation | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_work_111",
            no_task="if_log_get_u_d_f_uri_for_work_111_blank_112",
        )

        log_get_u_d_f_uri_for_work_111 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_work_111',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_66'), 'displayText', 'Work', 'uri')
        )

        if_log_get_u_d_f_uri_for_work_111_blank_112 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_work_111_blank_112',
            test='''{{ result('log_get_u_d_f_uri_for_work_111') | is_falsy }}''',
            yes_task="insert_to_list_113",
            no_task="updated_u_d_ffor_work_115",
        )

        insert_to_list_113 = rail.SetVariableOperator(
            task_id='insert_to_list_113',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Work udf is not available"
            }
        )

        updated_u_d_ffor_work_115 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_work_115',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_30').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_work_111') }}",
                "value": "{{ dag_run.conf.WorkLocation }}"
            }
        )

        if_request_locationname_present_work_location_116 = rail.IfOperator(
            task_id='if_request_locationname_present_work_location_116',
            test='''{{ dag_run.conf.LocationName | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_work_location_117",
            no_task="if_log_get_u_d_f_uri_for_work_location_117_blank_118",
        )

        log_get_u_d_f_uri_for_work_location_117 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_work_location_117',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_66'), 'displayText', 'Work Location', 'uri')
        )

        if_log_get_u_d_f_uri_for_work_location_117_blank_118 = rail.IfOperator(
            task_id='if_log_get_u_d_f_uri_for_work_location_117_blank_118',
            test='''{{ result('log_get_u_d_f_uri_for_work_location_117') | is_falsy }}''',
            yes_task="insert_to_list_119",
            no_task="updated_u_d_ffor_work_location_121",
        )

        insert_to_list_119 = rail.SetVariableOperator(
            task_id='insert_to_list_119',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Work Location udf is not available"
            }
        )

        updated_u_d_ffor_work_location_121 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_work_location_121',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_30').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_work_location_117') }}",
                "value": "{{ dag_run.conf.LocationName }}"
            }
        )

        if_request_suspendassignmentcategory_present_102 = rail.IfOperator(
            task_id='if_request_suspendassignmentcategory_present_102',
            test='''{{ dag_run.conf.SuspendAssignmentCategory | is_truthy }}''',
            yes_task="log_get_u_d_f_uri_for_suspend_assignment_category_103",
            no_task="if_request_overtimeeligibility_present",
        )

        log_get_u_d_f_uri_for_suspend_assignment_category_103 = rail.PythonOperator(
            task_id='log_get_u_d_f_uri_for_suspend_assignment_category_103',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_66'), 'displayText', 'Suspend Assignment Category', 'uri')
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
                "log": '''Suspend Assignment Category value {{ dag_run.conf.AssignmentCategory }} is not available in Replicon'''
            }
        )

        updated_u_d_ffor_suspend_assignment_category_112 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_suspend_assignment_category_112',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user_30').uri }}",
                "customFieldUri": "{{ result('log_get_u_d_f_uri_for_suspend_assignment_category_103') }}",
                "customFieldDropDownOptionUri": "{{ result('log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108') }}"
            }
        )

        if_request_overtimeeligibility_present = rail.IfOperator(
            task_id='if_request_overtimeeligibility_present',
            test='''{{ dag_run.conf.OvertimeEligibility | is_truthy }}''',
            yes_task="log_get_udf_uri_for_overtimeeligibility",
            no_task="if_request_industryfocusgroup_present_113",
        )

        log_get_udf_uri_for_overtimeeligibility = rail.PythonOperator(
            task_id='log_get_udf_uri_for_overtimeeligibility',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_66'), 'displayText', "Overtime Eligibility", 'uri')
        )

        if_pluckuri_first_blank = rail.IfOperator(
            task_id='if_pluckuri_first_blank',
            test='''{{ result('log_get_udf_uri_for_overtimeeligibility') | is_falsy }}''',
            yes_task="insert_to_list_108",
            no_task="_adhoc_http_action_ddoptions",
        )

        insert_to_list_108 = rail.SetVariableOperator(
            task_id='insert_to_list_108',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": "Overtime Eligibility udf is not available"
            }
        )

        _adhoc_http_action_ddoptions = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_ddoptions',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": '''{{ result('log_get_udf_uri_for_overtimeeligibility') }}'''
            }
        )

        log_uriforovertimeeligibility = rail.PythonOperator(
            task_id='log_uriforovertimeeligibility',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_ddoptions'), 'displayText', dag_run.conf['OvertimeEligibility'], 'uri')
        )

        if_pluckuri_firstnil_blank = rail.IfOperator(
            task_id='if_pluckuri_firstnil_blank',
            test='''{{ result('log_uriforovertimeeligibility') | is_falsy }}''',
            yes_task="insert_to_list_112",
            no_task="updated_u_d_ffor_overtime_eligibility",
        )

        insert_to_list_112 = rail.SetVariableOperator(
            task_id='insert_to_list_112',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": '''Overtime Eligibility value "{{dag_run.conf.OvertimeEligibility}}" is not available in Replicon'''
            }
        )

        updated_u_d_ffor_overtime_eligibility = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_overtime_eligibility',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user_30').uri }}",
                "customFieldUri": "{{ result('log_get_udf_uri_for_overtimeeligibility') }}",
                "customFieldDropDownOptionUri": '''{{ result('log_uriforovertimeeligibility') }}'''
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
            no_task="insert_to_list_118",
        )

        put_industry_focus_group_schedule_for_user_division_117 = rail.RepliconServiceOperator(
            task_id='put_industry_focus_group_schedule_for_user_division_117',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data={
                "userUri": "{{ result('create_user_30').uri }}",
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

        insert_to_list_118 = rail.SetVariableOperator(
            task_id='insert_to_list_118',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": '''Industry Focus Group {{dag_run.conf.IndustryFocusGroup}} is not available in Replicon'''
            }
        )

        # pylint: disable=too-many-boolean-expressions
        def get_schedule_name(dag_run):
            default_schedule_name = None
            if dag_run.conf['DWSMonday'] == "0" and dag_run.conf['DWSTuesday'] == "0" and dag_run.conf['DWSWednesday'] == "0" and dag_run.conf['DWSThursday'] == "0" and dag_run.conf['DWSFriday'] == "0" and dag_run.conf['DWSSaturday'] == "0" and dag_run.conf['DWSSunday'] == "0":
                default_schedule_name = get_entity_from_mapper(
                    dag_run, "Default Schedule")
            elif dag_run.conf['DWSMonday'] and dag_run.conf['DWSTuesday'] \
                and dag_run.conf['DWSWednesday'] and dag_run.conf['DWSThursday'] \
                    and dag_run.conf['DWSFriday'] and dag_run.conf['DWSSaturday'] \
                and dag_run.conf['DWSSunday']:
                default_schedule_name = dag_run.conf['DWSMonday'] + "|" + dag_run.conf['DWSTuesday'] + "|" + dag_run.conf['DWSWednesday'] + "|" + \
                    dag_run.conf['DWSThursday'] + "|" + dag_run.conf['DWSFriday'] + \
                    "|" + dag_run.conf['DWSSaturday'] + \
                    "|" + dag_run.conf['DWSSunday']
            elif dag_run.conf['DWSMonday'] is None or dag_run.conf['DWSTuesday'] is None or dag_run.conf['DWSWednesday'] is None or dag_run.conf['DWSThursday'] is None or dag_run.conf['DWSFriday'] is None or dag_run.conf['DWSSaturday'] is None or dag_run.conf['DWSSunday'] is None:
                default_schedule_name = get_entity_from_mapper(
                    dag_run, "Default Schedule")
            return default_schedule_name

        log_office_schedulename_120 = rail.PythonOperator(
            task_id='log_office_schedulename_120',
            python_callable=get_schedule_name
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
            no_task="insert_to_list_161",
        )

        assign_initial_schedule_124 = rail.RepliconServiceOperator(
            task_id='assign_initial_schedule_124',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user_30').uri }}",
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

        insert_to_list_161 = rail.SetVariableOperator(
            task_id='insert_to_list_161',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "log": '''Schedule {{ result('log_office_schedulename_120') }}" is not available in Replicon'''
            }
        )

        # pylint: disable=too-many-boolean-expressions
        def get_numberofhours(dag_run):
            numberofhours = 0
            schedule_name = rail.result('log_office_schedulename_120')
            default_schedule_name = get_entity_from_mapper(
                dag_run, "Default Schedule")
            if default_schedule_name == schedule_name:
                numberofhours = 40
            else:
                if dag_run.conf['DWSMonday'] and float(dag_run.conf['DWSMonday']) > 0:
                    numberofhours += float(dag_run.conf['DWSMonday'])
                if dag_run.conf['DWSTuesday'] and float(dag_run.conf['DWSTuesday']) > 0:
                    numberofhours += float(dag_run.conf['DWSTuesday'])
                if dag_run.conf['DWSWednesday'] and float(dag_run.conf['DWSWednesday']) > 0:
                    numberofhours += float(dag_run.conf['DWSWednesday'])
                if dag_run.conf['DWSThursday'] and float(dag_run.conf['DWSThursday']) > 0:
                    numberofhours += float(dag_run.conf['DWSThursday'])
                if dag_run.conf['DWSFriday'] and float(dag_run.conf['DWSFriday']) > 0:
                    numberofhours += float(dag_run.conf['DWSFriday'])
                if dag_run.conf['DWSSaturday'] and float(dag_run.conf['DWSSaturday']) > 0:
                    numberofhours += float(dag_run.conf['DWSSaturday'])
                if dag_run.conf['DWSSunday'] and float(dag_run.conf['DWSSunday']) > 0:
                    numberofhours += float(dag_run.conf['DWSSunday'])
            return numberofhours

        trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_type_for_new_user_v1_0181 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_type_for_new_user_v1_0181',
            retries=0,
            items=[-1],
            trigger_dag_id=f'ge_user_sync_netherlands_child_workflow_to_add_timeoff_type_for_new_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['OHRID'],
                "useruri": rail.result('create_user_30')['uri'],
                "legalentity": dag_run.conf['LegalEntity'],
                "startdate": dag_run.conf['LegalEntityHireDate'] if dag_run.conf['LegalEntityHireDate'] else dag_run.conf['HireEffectiveDate'],
                "type": "Add",
                "fullpart": "Full Time" if get_numberofhours(dag_run) > 39 else "Part Time",
                "legacypayrollid": dag_run.conf['LegacyPayrollID'],
                "jobtype": dag_run.conf['JobType'],
                "payrule": dag_run.conf['Payroll'],
                "scheduledweeklyhours": 40 if get_numberofhours(dag_run) > 40 else get_numberofhours(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_type_for_new_user_v1_0181 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_type_for_new_user_v1_0181',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_type_for_new_user_v1_0181") }}'
        )

        def get_validation_message(dag_run):
            final_validation = ""
            validation = rail.get_dag_run_var(
                rail.result('declare_list_2')['name'])
            validations = [v['log'] for v in validation]
            if validations:
                final_validation = "New user profile added partially as " + \
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
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> ey_user_import_logs_add_entry_141
        can_run_batch_task >> rail.Label('No') >> declare_list_2
        declare_list_2 >> log_checkifrequiredfieldsarenotthere_4 >> if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5
        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_6 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_139
        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 >> rail.Label(
            'No') >> netherlands_master_mapper_search_entries_8 >> if_first_id_blank_9
        if_first_id_blank_9 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_10 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_139
        if_first_id_blank_9 >> rail.Label(
            'No') >> log_employee_type_name_from_mapper_12 >> log_mapper_entry >> if_log_employee_type_name_from_mapper_12_blank_13
        if_log_employee_type_name_from_mapper_12_blank_13 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_14 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_139
        if_log_employee_type_name_from_mapper_12_blank_13 >> rail.Label(
            'No') >> if_request_departmenturi_blank_16
        if_request_departmenturi_blank_16 >> rail.Label('No') >> create_user_30
        if_request_departmenturi_blank_16 >> rail.Label(
            'Yes') >> ey_user_import_logs_add_entry_17 >> log_to_sumo
        create_user_30 >> remove_timeoff_assignments_44 >> put_product_assignments_for_user_46 >> update_language_39 >> \
            if_request_overtimeeligibility_contains_yes_36
        if_request_overtimeeligibility_contains_yes_36 >> rail.Label('Yes') >> trigger_dag_run_ge_netherlands_payrule_assignment_add_update_v1_038 >> \
            wait_for_completion_trigger_dag_run_ge_netherlands_payrule_assignment_add_update_v1_038 >> if_request_supervisorssoid_present_57
        if_request_overtimeeligibility_contains_yes_36 >> rail.Label(
            'No') >> if_request_supervisorssoid_present_57
        if_request_supervisorssoid_present_57 >> rail.Label(
            'Yes') >> if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_58
        if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_58 >> rail.Label(
            'Yes') >> insert_to_list_59 >> _adhoc_http_action_66
        if_request_supervisorssoid_equals_to_dataworkato_servicereceive_requestrequestohrid_58 >> rail.Label(
            'No') >> if_request_supervisorssoid_present_61
        if_request_supervisorssoid_present_61 >> rail.Label(
            'Yes') >> search_users_search_supervisor_62 >> if_log_supervisor_uri_63_blank_64
        if_log_supervisor_uri_63_blank_64 >> rail.Label(
            'Yes') >> ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_65 >> _adhoc_http_action_66
        if_log_supervisor_uri_63_blank_64 >> rail.Label(
            'No') >> if_downcase_not_equals_to_true_68
        if_downcase_not_equals_to_true_68 >> rail.Label(
            'Yes') >> ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_69 >> _adhoc_http_action_66
        if_downcase_not_equals_to_true_68 >> rail.Label(
            'No') >> _adhoc_http_action_search_supervisor_71 >> log_required_supervisor_permission_72 >> \
            log_supervisorpermissionassignedtouser_73 >> log_end_userpermissionassignedtouser_74 >> if_log_supervisorpermissionassignedtouser_73_blank_75
        if_log_supervisorpermissionassignedtouser_73_blank_75 >> rail.Label(
            'Yes') >> _adhoc_http_action_search_supervisor_76 >> get_permission_uris >> \
            should_add_missing_permissions
        should_add_missing_permissions >> rail.Label(
            'No') >> assign_initital_supervisor_81
        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_supervisor_permissions >> assign_initital_supervisor_81
        if_log_supervisorpermissionassignedtouser_73_blank_75 >> rail.Label(
            'No') >> assign_initital_supervisor_81 >> _adhoc_http_action_66
        if_request_supervisorssoid_present_61 >> rail.Label(
            'No') >> _adhoc_http_action_66
        if_request_supervisorssoid_present_57 >> rail.Label(
            'No') >> _adhoc_http_action_66 >> if_request_hrmssoid_present_67
        if_request_hrmssoid_present_67 >> rail.Label(
            'Yes') >> log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84 >> if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84_blank_85
        if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84_blank_85 >> rail.Label(
            'Yes') >> insert_to_list_86 >> if_request_hrmname_present_89
        if_log_get_u_d_f_uri_for_h_r_m_s_s_o_i_d_84_blank_85 >> rail.Label(
            'No') >> updated_u_d_ffor_h_r_m_s_s_o_i_d_88 >> if_request_hrmname_present_89
        if_request_hrmssoid_present_67 >> rail.Label(
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
            'Yes') >> insert_to_list_99 >> if_request_payroll_present_86
        if_log_get_u_d_f_uri_for_job_position_title_97_blank_98 >> rail.Label(
            'No') >> updated_u_d_ffor_job_position_title_101 >> if_request_payroll_present_86
        if_request_jobpositiontitle_present_96 >> rail.Label(
            'No') >> if_request_payroll_present_86
        if_request_payroll_present_86 >> rail.Label(
            'Yes') >> log_get_u_d_f_uri_for_payroll_87 >> if_log_get_u_d_f_uri_for_payroll_87_blank_88
        if_log_get_u_d_f_uri_for_payroll_87_blank_88 >> rail.Label(
            'No') >> insert_to_list_89 >> if_request_healthcareproductlineeit_present_92
        if_log_get_u_d_f_uri_for_payroll_87_blank_88 >> rail.Label(
            'Yes') >> updated_u_d_ffor_payroll_91 >> if_request_healthcareproductlineeit_present_92
        if_request_healthcareproductlineeit_present_92 >> rail.Label(
            'No') >> if_request_jobtype_present_98
        if_request_healthcareproductlineeit_present_92 >> rail.Label(
            'Yes') >> log_get_u_d_f_uri_for_healthcare_product_lineeit_93 >> if_log_get_u_d_f_uri_for_healthcare_product_lineeit_93_blank_94
        if_log_get_u_d_f_uri_for_healthcare_product_lineeit_93_blank_94 >> rail.Label(
            'No') >> updated_u_d_ffor_healthcare_product_line_e_i_t_97 >> if_request_jobtype_present_98
        if_request_jobtype_present_98 >> rail.Label(
            'Yes') >> log_get_u_d_f_uri_for_job_type_99 >> if_log_get_u_d_f_uri_for_job_type_99_blank_100
        if_log_get_u_d_f_uri_for_job_type_99_blank_100 >> rail.Label(
            'No') >> updated_u_d_ffor_job_type_103 >> if_request_careerband_present_104
        if_log_get_u_d_f_uri_for_job_type_99_blank_100 >> rail.Label(
            'Yes') >> insert_to_list_101 >> if_request_careerband_present_104
        if_request_careerband_present_104 >> rail.Label(
            'Yes') >> if_request_work_present_110
        if_request_careerband_present_104 >> rail.Label(
            'No') >> log_get_u_d_f_uri_for_career_band_105 >> if_log_get_u_d_f_uri_for_career_band_105_blank_106
        if_log_get_u_d_f_uri_for_career_band_105_blank_106 >> rail.Label(
            'Yes') >> insert_to_list_107 >> if_request_work_present_110
        if_log_get_u_d_f_uri_for_career_band_105_blank_106 >> rail.Label(
            'No') >> updated_u_d_ffor_career_band_109 >> if_request_work_present_110
        if_request_work_present_110 >> rail.Label(
            'No') >> if_log_get_u_d_f_uri_for_work_111_blank_112
        if_request_work_present_110 >> rail.Label(
            'Yes') >> log_get_u_d_f_uri_for_work_111 >> if_log_get_u_d_f_uri_for_work_111_blank_112
        if_request_jobtype_present_98 >> rail.Label(
            'No') >> if_request_careerband_present_104
        if_log_get_u_d_f_uri_for_work_111_blank_112 >> rail.Label(
            'Yes') >> insert_to_list_113 >> if_request_locationname_present_work_location_116
        if_log_get_u_d_f_uri_for_work_111_blank_112 >> rail.Label(
            'No') >> updated_u_d_ffor_work_115 >> if_request_locationname_present_work_location_116
        if_request_locationname_present_work_location_116 >> rail.Label(
            'Yes') >> log_get_u_d_f_uri_for_work_location_117 >> if_log_get_u_d_f_uri_for_work_location_117_blank_118
        if_request_locationname_present_work_location_116 >> rail.Label(
            'No') >> if_log_get_u_d_f_uri_for_work_location_117_blank_118
        if_log_get_u_d_f_uri_for_work_location_117_blank_118 >> rail.Label(
            'No') >> updated_u_d_ffor_work_location_121 >> if_request_suspendassignmentcategory_present_102
        if_log_get_u_d_f_uri_for_work_location_117_blank_118 >> rail.Label('Yes') >> insert_to_list_119 >> \
            if_request_suspendassignmentcategory_present_102
        if_log_get_u_d_f_uri_for_healthcare_product_lineeit_93_blank_94 >> rail.Label(
            'Yes') >> insert_to_list_95 >> if_request_jobtype_present_98
        if_request_payroll_present_86 >> rail.Label(
            'No') >> if_request_healthcareproductlineeit_present_92
        if_request_suspendassignmentcategory_present_102 >> rail.Label(
            'Yes') >> log_get_u_d_f_uri_for_suspend_assignment_category_103 >> if_log_get_u_d_f_uri_for_suspend_assignment_category_103_blank_104
        if_log_get_u_d_f_uri_for_suspend_assignment_category_103_blank_104 >> rail.Label(
            'Yes') >> insert_to_list_105 >> if_request_overtimeeligibility_present
        if_log_get_u_d_f_uri_for_suspend_assignment_category_103_blank_104 >> rail.Label(
            'No') >> _adhoc_http_action_107 >> log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108 >> \
            if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108_blank_109
        if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108_blank_109 >> rail.Label(
            'Yes') >> insert_to_list_110 >> if_request_overtimeeligibility_present
        if_log_get_u_d_fvalue_uri_for_suspend_assignment_categoryvalue_108_blank_109 >> rail.Label(
            'No') >> updated_u_d_ffor_suspend_assignment_category_112 >> if_request_overtimeeligibility_present
        if_request_overtimeeligibility_present >> rail.Label(
            'No') >> if_request_industryfocusgroup_present_113
        if_request_overtimeeligibility_present >> rail.Label(
            'Yes') >> log_get_udf_uri_for_overtimeeligibility >> \
            if_pluckuri_first_blank
        if_pluckuri_first_blank >> rail.Label(
            'No') >> insert_to_list_108 >> if_request_industryfocusgroup_present_113
        if_pluckuri_first_blank >> rail.Label('Yes') >> _adhoc_http_action_ddoptions >> \
            log_uriforovertimeeligibility >> if_pluckuri_firstnil_blank
        if_pluckuri_firstnil_blank >> rail.Label(
            'No') >> insert_to_list_112 >> if_request_industryfocusgroup_present_113
        if_pluckuri_firstnil_blank >> rail.Label(
            'Yes') >> updated_u_d_ffor_overtime_eligibility >> if_request_industryfocusgroup_present_113
        if_request_suspendassignmentcategory_present_102 >> rail.Label(
            'No') >> if_request_overtimeeligibility_present
        if_request_industryfocusgroup_present_113 >> rail.Label(
            'Yes') >> _adhoc_http_action_114 >> log_gettherequired_industry_focus_groupvalue_uri_115 >> \
            if_log_gettherequired_industry_focus_groupvalue_uri_115_present_116
        if_log_gettherequired_industry_focus_groupvalue_uri_115_present_116 >> rail.Label(
            'Yes') >> put_industry_focus_group_schedule_for_user_division_117 >> log_office_schedulename_120
        if_log_gettherequired_industry_focus_groupvalue_uri_115_present_116 >> rail.Label(
            'No') >> insert_to_list_118 >> log_office_schedulename_120
        if_request_industryfocusgroup_present_113 >> rail.Label(
            'No') >> log_office_schedulename_120 >> _adhoc_http_action_121 >> log_gettherequiredofficeschedule_uri_122 >> \
            if_log_gettherequiredofficeschedule_uri_122_present_123
        if_log_gettherequiredofficeschedule_uri_122_present_123 >> rail.Label(
            'No') >> insert_to_list_161 >> trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_type_for_new_user_v1_0181 >> \
            wait_for_completion_trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_type_for_new_user_v1_0181 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_139
        if_log_gettherequiredofficeschedule_uri_122_present_123 >> rail.Label(
            'Yes') >> assign_initial_schedule_124 >> trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_type_for_new_user_v1_0181
        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_6_6_139 >> ey_user_import_logs_add_entry_141 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)

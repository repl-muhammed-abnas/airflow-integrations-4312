from datetime import timedelta
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from momentive.user_import_india.utils import python_callable, request_payload
from momentive.user_import_india.mappers.momentive_user_import_mapper import momentive_userimport_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_india_user_sync_child_add_user_dag_id,
        description=f'Momentive_India_User Sync Add_Child {config.instance}',
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
            no_task='get_input_validation_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_input_validation_log',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_input_validation_log = rail.PythonOperator(
            task_id="get_input_validation_log",
            python_callable=python_callable.get_input_validationlog
        )

        if_input_validation_log_present = rail.IfOperator(
            task_id='if_input_validation_log_present',
            test="{{ result('get_input_validation_log').exc_present | is_truthy }}",
            yes_task="log_user_import_not_created",
            no_task="if_workertype_not_contingentworker_and_businesstitle_not_present",
        )

        log_user_import_not_created = rail.WriteLogOperator(
            task_id="log_user_import_not_created",
            log='{{ dag_run.conf.user_import_logs}}',
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + "|" + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Exception",
                "details": "User not created ; " + rail.result('get_input_validation_log')['exc_value'],
                "childjobid": get_dagrun_ecid(dag_run),
            }
        )

        if_workertype_not_contingentworker_and_businesstitle_not_present = rail.IfOperator(
            task_id='if_workertype_not_contingentworker_and_businesstitle_not_present',
            test=lambda dag_run: dag_run.conf['workertype'] == 'Contingent Worker' and not ((dag_run.conf['businesstitle'].lower() if dag_run.conf['businesstitle'] else "Null").startswith(
                "ext")),
            yes_task="log_user_import_not_created_contingent_non_alp",
            no_task="get_all_employee_type",
        )

        log_user_import_not_created_contingent_non_alp = rail.WriteLogOperator(
            task_id="log_user_import_not_created_contingent_non_alp",
            log='{{ dag_run.conf.user_import_logs}}',
            message="Exception",
            severity="Exception",
            properties=lambda dag_run: {
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.userid }}",
                "username": "{{ dag_run.conf.firstname }}" + "|" + "{{ dag_run.conf.lastname }}",
                "action": "Add",
                "status": "Exception",
                'details': "User not created since user belongs to Contingent non ALP group",
                "childjobid": get_dagrun_ecid(dag_run),
            }
        )

        get_all_employee_type = rail.RepliconServiceOperator(
            task_id="get_all_employee_type",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups"
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: {
                'basic_user_with_report_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', "Basic User with Reports", 'uri'),
                'supervisor': rail.find_first_by_attr_and_get_attr(
                    response, 'name', "Supervisor - Edit", 'uri')
            }
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        def log_businesstitle(dag_run):
            if dag_run.conf['location'] == 'IN Chennai':
                return "Trainee" if dag_run.conf['businesstitle'].lower().startswith("trainee") else (
                    "EXT" if dag_run.conf['businesstitle'].lower().startswith("ext") else (
                        "DCS" if dag_run.conf['businesstitle'].lower().startswith("dcs") else "Any"))
            else:
                return "Any" if dag_run.conf['businesstitle'].lower().startswith("trainee") else (
                    "EXT" if dag_run.conf['businesstitle'].lower().startswith("ext") else (
                        "Any" if dag_run.conf['businesstitle'].lower().startswith("dcs") else "Any"))

        log_businesstitle_18 = rail.PythonOperator(
            task_id='log_businesstitle_18',
            python_callable=log_businesstitle
        )

        momentive_userimport_mapper_search_entries_19 = rail.PythonOperator(
            task_id='momentive_userimport_mapper_search_entries_19',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "Employee Type" and x["workertype"] == dag_run.conf['workertype'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and (
                x['businesstitle'] == (rail.result('log_businesstitle_18') if rail.result('log_businesstitle_18') else x['businesstitle'])), momentive_userimport_mapper))
        )

        if_mapper_search_entry_present = rail.IfOperator(
            task_id='if_mapper_search_entry_present',
            test='''{{ result('momentive_userimport_mapper_search_entries_19')| is_truthy }}''',
            yes_task="get_required_employeetype_uri",
            no_task="if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present",
        )

        get_required_employeetype_uri = rail.PythonOperator(
            task_id="get_required_employeetype_uri",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_employee_type'), 'displayText', rail.result(
                    'momentive_userimport_mapper_search_entries_19')[0]['value'], 'uri', '')
        )

        if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present = rail.IfOperator(
            task_id='if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present',
            test="{{ result('get_required_employeetype_uri')| is_falsy or \
                dag_run.conf.departmentgroupuri | is_falsy }}",
            yes_task="details_employeetype_and_departmentygrpuri_not_exist",
            no_task="if_entry_in_mapper_37_value_present_and_deptgrpuri_present",
        )

        details_employeetype_and_departmentygrpuri_not_exist = rail.PythonOperator(
            task_id='details_employeetype_and_departmentygrpuri_not_exist',
            python_callable=python_callable.get_details_for_employeetype_and_departmentygrpuri_not_exist
        )

        log_user_import_employeetype_dept_not_exist = rail.WriteLogOperator(
            task_id="log_user_import_employeetype_dept_not_exist",
            log='{{ dag_run.conf.user_import_logs}}',
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + "|" + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Exception",
                "details": rail.smartjoin_by_delim(rail.result('details_employeetype_and_departmentygrpuri_not_exist').split(";"), ";"),
                "childjobid": get_dagrun_ecid(dag_run),
            }
        )

        if_entry_in_mapper_37_value_present_and_deptgrpuri_present = rail.IfOperator(
            task_id='if_entry_in_mapper_37_value_present_and_deptgrpuri_present',
            test="{{ result('get_required_employeetype_uri')| is_truthy and \
                dag_run.conf.departmentgroupuri | is_truthy }}",
            yes_task="get_all_pay_rule_scripts_28",
            no_task="catch_and_log_error",
        )

        get_all_pay_rule_scripts_28 = rail.RepliconServiceOperator(
            task_id='get_all_pay_rule_scripts_28',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        momentive_userimport_mapper_search_entries_29 = rail.PythonOperator(
            task_id='momentive_userimport_mapper_search_entries_29',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["location"] == dag_run.conf['location'], momentive_userimport_mapper))
        )

        log_timesheettemplatetobeassigned_30 = rail.PythonOperator(
            task_id='log_timesheettemplatetobeassigned_30',
            python_callable=lambda dag_run: next((x['value'] for x in filter(lambda x: x["type"] == "Timesheet Template" and x["workertype"] == dag_run.conf['workertype'] and
                                                                             x["location"] == dag_run.conf['location'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and x['businesstitle'] == rail.result(
                    'log_businesstitle_18'), rail.result('momentive_userimport_mapper_search_entries_29') or [])), '')
        )

        create_payrule_variable = rail.SetVariableOperator(
            task_id='create_payrule_variable',
            append=False,
            name='payrule',
            value=''
        )

        if_request_location_equals_to_inbangalorembs_32 = rail.IfOperator(
            task_id='if_request_location_equals_to_inbangalorembs_32',
            test='''{{ dag_run.conf.location == 'IN Bangalore MBS' }}''',
            yes_task="if_request_india_spec_schedule_indicator_equals_to_yes_33",
            no_task="update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_m_b_s_38",
        )

        if_request_india_spec_schedule_indicator_equals_to_yes_33 = rail.IfOperator(
            task_id='if_request_india_spec_schedule_indicator_equals_to_yes_33',
            test='''{{ dag_run.conf.India_Spec_schedule_Indicator == 'Yes' }}''',
            yes_task="update_variable_when_india_spec_schedule_indicatorisyesandlocationis_m_b_s_34",
            no_task="update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_m_b_s_36",
        )

        update_variable_when_india_spec_schedule_indicatorisyesandlocationis_m_b_s_34 = rail.SetVariableOperator(
            task_id='update_variable_when_india_spec_schedule_indicatorisyesandlocationis_m_b_s_34',
            append=False,
            name='{{ result("create_payrule_variable").name }}',
            value=lambda dag_run: next((x['value'].strip() for x in filter(
                lambda x: x["type"] == "Payrule" and x["workertype"] == dag_run.conf['workertype'] and x["location"] == dag_run.conf['location'] and (
                    x["exemptstatus"] == dag_run.conf['exemptionstatus']) and x['businesstitle'] == "Premium", rail.result(
                        'momentive_userimport_mapper_search_entries_29') or [])), '')
        )

        update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_m_b_s_36 = rail.SetVariableOperator(
            task_id='update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_m_b_s_36',
            append=False,
            name='{{ result("create_payrule_variable").name }}',
            value=lambda dag_run: next((x['value'].strip() for x in filter(
                lambda x: x["type"] == "Payrule" and x["workertype"] == dag_run.conf['workertype'] and x["location"] == dag_run.conf['location'] and (
                    x["exemptstatus"] == dag_run.conf['exemptionstatus']) and x['businesstitle'] == rail.result('log_businesstitle_18'), rail.result(
                        'momentive_userimport_mapper_search_entries_29') or [])), '')
        )

        update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_m_b_s_38 = rail.SetVariableOperator(
            task_id='update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_m_b_s_38',
            append=False,
            name='{{ result("create_payrule_variable").name }}',
            value=lambda dag_run: next((x['value'].strip() for x in filter(
                lambda x: x["type"] == "Payrule" and x["workertype"] == dag_run.conf['workertype'] and x["location"] == dag_run.conf['location'] and (
                    x["exemptstatus"] == dag_run.conf['exemptionstatus']) and x['businesstitle'] == rail.result('log_businesstitle_18'), rail.result(
                        'momentive_userimport_mapper_search_entries_29') or [])), '')
        )

        log_punch_entry_policytobeassigned_39 = rail.PythonOperator(
            task_id='log_punch_entry_policytobeassigned_39',
            python_callable=lambda dag_run: next((x['value'].strip() for x in filter(
                lambda x: x["type"] == "Punch Entry Policy" and x["workertype"] == dag_run.conf['workertype'] and x["location"] == dag_run.conf['location'] and (
                    x["exemptstatus"] == dag_run.conf['exemptionstatus']) and x['businesstitle'] == rail.result('log_businesstitle_18'), rail.result(
                    'momentive_userimport_mapper_search_entries_29') or [])), '')
        )

        log_pay_rule_uri_40 = rail.PythonOperator(
            task_id='log_pay_rule_uri_40',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_pay_rule_scripts_28'), 'displayText', rail.get_dag_run_var('payrule'), 'uri', '')
        )

        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['userid'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": dag_run.conf['emailaddress'],
                    "employeeId": dag_run.conf['Worker_Reference_Employee_ID'],
                    "department": null,
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
                    "employmentDateRange": {
                        "startDate": python_callable.split_date_string(dag_run.conf['hiredate']),
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['userid'],
                        "SSOName": dag_run.conf['userid'],
                        "password": null
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": [
                        {
                            "uri": rail.result('get_all_permissionsets')['basic_user_with_report_uri'],
                            "name": null
                        }
                    ],
                    "policySets": [
                        {
                            "uri": null,
                            "name": rail.result('log_timesheettemplatetobeassigned_30')
                        },
                        {
                            "uri": null,
                            "name": "IND_Time Off"
                        },
                        {
                            "uri": null,
                            "name": rail.result('log_punch_entry_policytobeassigned_39')
                        }
                    ],
                    "employeeType": null,
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
                        "uri": null,
                        "IANAName": "Asia/Calcutta"
                    },
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "departmentGroupSchedule": [
                        {
                            "departmentGroup": {
                                "uri": dag_run.conf['departmentgroupuri'],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "employeeTypeGroupSchedule": [
                        {
                            "employeeTypeGroup": {
                                "uri": rail.result('get_required_employeetype_uri'),
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
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
                    "payRuleScriptSchedule": [
                        {
                            "payRuleScript": {
                                "uri": rail.result('log_pay_rule_uri_40'),
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ]
                }
            }
        )

        remove_all_timeoffs = rail.RepliconServiceOperator(
            task_id='remove_all_timeoffs',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ result('create_user').uri }}",
                'timeOffTypeUris': []
            }
        )

        get_all_policy_sets_46 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_46',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'date_of_birth_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Date of Birth', 'uri', ''),
                'title_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Title', 'uri', ''),
                'workersubtypeuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Worker Sub Type', 'uri', ''),
                'year_of_service_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Years of Service', 'uri', ''),
                'hrm_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'HRM', 'uri', ''),
                'continous_years_of_service_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Continuous Years of Service - YOS', 'uri', ''),
                'timeoffservicedate_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Time off Service Date - YOSS', 'uri', ''),
                'gender_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Gender', 'uri', ''),
                'function_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Function', 'uri', ''),
                'workshift_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Work Shift', 'uri', '')
            }
        )

        if_CF_Date_of_Birth_MM_DD_YYYY_and_dob_uri_present = rail.IfOperator(
            task_id='if_CF_Date_of_Birth_MM_DD_YYYY_and_dob_uri_present',
            test="{{ dag_run.conf.CF_Date_of_Birth_MM_DD_YYYY | is_truthy }}",
            yes_task="if_log_gettherequiredcustomfield_urifor_dateof_birth_49_present_50",
            no_task="if_businesstitle_present",
        )

        if_log_gettherequiredcustomfield_urifor_dateof_birth_49_present_50 = rail.IfOperator(
            task_id='if_log_gettherequiredcustomfield_urifor_dateof_birth_49_present_50',
            test='''{{ result('get_required_user_customfields').date_of_birth_uri | is_truthy }}''',
            yes_task="update_dob_udf",
            no_task="if_businesstitle_present",
        )

        update_dob_udf = rail.RepliconServiceOperator(
            task_id='update_dob_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": rail.result('get_required_user_customfields')['date_of_birth_uri'],
                "value": python_callable.split_date_string(dag_run.conf['CF_Date_of_Birth_MM_DD_YYYY'])
            }
        )

        if_businesstitle_present = rail.IfOperator(
            task_id='if_businesstitle_present',
            test="{{ dag_run.conf.businesstitle | is_truthy }}",
            yes_task="if_log_gettherequiredcustomfield_urifor_title_54_present_55",
            no_task="if_fieldhr_present",
        )

        if_log_gettherequiredcustomfield_urifor_title_54_present_55 = rail.IfOperator(
            task_id='if_log_gettherequiredcustomfield_urifor_title_54_present_55',
            test='''{{result('get_required_user_customfields').title_uri | is_truthy }}''',
            yes_task="update_title_udf",
            no_task="if_fieldhr_present",
        )

        update_title_udf = rail.RepliconServiceOperator(
            task_id='update_title_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').title_uri }}",
                "value": "{{ dag_run.conf.businesstitle }}"
            }
        )

        if_fieldhr_present = rail.IfOperator(
            task_id='if_fieldhr_present',
            test="{{ dag_run.conf.fieldhr | is_truthy}}",
            yes_task="if_log_gettherequiredcustomfield_urifor_h_r_m_58_present_59",
            no_task="if_gender_and_genderuri_present",
        )

        if_log_gettherequiredcustomfield_urifor_h_r_m_58_present_59 = rail.IfOperator(
            task_id='if_log_gettherequiredcustomfield_urifor_h_r_m_58_present_59',
            test='''{{ result('get_required_user_customfields').hrm_uri | is_truthy }}''',
            yes_task="update_fieldhr_udf",
            no_task="if_gender_and_genderuri_present",
        )

        update_fieldhr_udf = rail.RepliconServiceOperator(
            task_id='update_fieldhr_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').hrm_uri }}",
                "value": "{{ dag_run.conf.fieldhr }}"
            }
        )

        if_gender_and_genderuri_present = rail.IfOperator(
            task_id='if_gender_and_genderuri_present',
            test="{{ dag_run.conf.gender | is_truthy and \
                result('get_required_user_customfields').gender_uri | is_truthy }}",
            yes_task="update_gender_udf",
            no_task="if_manager_id_present",
        )

        update_gender_udf = rail.RepliconServiceOperator(
            task_id='update_gender_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').gender_uri }}",
                "value": "{{ dag_run.conf.gender }}"
            }
        )

        if_manager_id_present = rail.IfOperator(
            task_id='if_manager_id_present',
            test="{{ dag_run.conf.managerid | is_truthy }}",
            yes_task="search_for_user_with_empid",
            no_task="get_timesheet_for_date2_85",
        )

        search_for_user_with_empid = rail.RepliconServiceOperator(
            task_id='search_for_user_with_empid',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.search_supervisor_payload,
            data_handler=python_callable.get_userdata_list_for_managerid
        )

        if_login_name_uri_present_71 = rail.IfOperator(
            task_id='if_login_name_uri_present_71',
            test='''{{result('search_for_user_with_empid') | is_truthy }}''',
            yes_task="get_manager_details",
            no_task="log_supervisor_assignment",
        )

        get_manager_details = rail.RepliconServiceOperator(
            task_id='get_manager_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [
                    {
                        "uri": rail.result('search_for_user_with_empid')[0]['uri']
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_manager_details_present_and_enabled = rail.IfOperator(
            task_id='if_manager_details_present_and_enabled',
            test="{{ result('get_manager_details') | is_truthy and result('get_manager_details')[0]['userDetails']['isEnabled'] | is_truthy }}",
            yes_task="get_assigned_permissionset_foruser",
            no_task="log_supervisor_assignment",
        )

        get_assigned_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionset_foruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_for_user_with_empid')[0].uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'user.uri', '')
        )

        if_supervisor_permission_not_assigned = rail.IfOperator(
            task_id='if_supervisor_permission_not_assigned',
            test="{{ result('get_assigned_permissionset_foruser') | is_falsy }}",
            yes_task="add_missing_supervisor_permission",
            no_task="update_initial_supervisor",
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=request_payload.add_missing_supervisor_permission_payload
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id="update_initial_supervisor",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "supervisorUri": "{{ result('search_for_user_with_empid')[0].uri }}",
                "dateRange": None
            }
        )

        log_supervisor_assignment = rail.WriteLogOperator(
            task_id="log_supervisor_assignment",
            log='{{ dag_run.conf.supervisor_assignment_logs}}',
            message="Exception",
            severity="Exception",
            properties=request_payload.supervisor_assignment_log_payload
        )

        get_timesheet_for_date2_85 = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2_85',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                "userUri": rail.result('create_user')['uri'],
                "date": python_callable.split_date_string(dag_run.conf['hiredate']),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        get_timesheet_details_86 = rail.RepliconServiceOperator(
            task_id='get_timesheet_details_86',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_timesheet_for_date2_85').timesheet.uri }}"
            }
        )

        log_work_weektobeassigned_87 = rail.PythonOperator(
            task_id='log_work_weektobeassigned_87',
            python_callable=lambda dag_run: next((x['value'].strip() for x in filter(lambda x: x["type"] == "Work Week" and x["workertype"] == dag_run.conf['workertype'] and
                                                                                     x["location"] == dag_run.conf['location'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and
                                                                                     x['businesstitle'] == rail.result('log_businesstitle_18'), rail.result(
                'momentive_userimport_mapper_search_entries_29') or [])), '')
        )

        get_allworkweek_88 = rail.RepliconServiceOperator(
            task_id='get_allworkweek_88',
            endpoint="/services/InternationalizationService1.svc/GetAllDaysOfWeek",
        )

        log_get_required_workweek_uri_89 = rail.PythonOperator(
            task_id='log_get_required_workweek_uri_89',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_allworkweek_88'), 'name', rail.result('log_work_weektobeassigned_87').split(" ")[0].strip(), 'uri', '')
        )

        if_log_get_required_workweek_uri_89_present_90 = rail.IfOperator(
            task_id='if_log_get_required_workweek_uri_89_present_90',
            test='''{{ result('log_get_required_workweek_uri_89') | is_truthy }}''',
            yes_task="update_work_week_for_user_91",
            no_task="get_all_office_schedules_94",
        )

        update_work_week_for_user_91 = rail.RepliconServiceOperator(
            task_id='update_work_week_for_user_91',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "dayOfWeekUri": "{{ result('log_get_required_workweek_uri_89') }}"
            }
        )

        get_all_office_schedules_94 = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules_94',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        log_scheduletobeassigned_95 = rail.PythonOperator(
            task_id='log_scheduletobeassigned_95',
            python_callable=lambda dag_run: next((x['value'].strip() for x in filter(lambda x: x["type"] == "Schedule" and x["workertype"] == dag_run.conf['workertype'] and
                                                                                     x["location"] == dag_run.conf['location'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and
                                                                                     x['businesstitle'] == rail.result('log_businesstitle_18'), rail.result(
                'momentive_userimport_mapper_search_entries_29') or [])), '')
        )

        if_log_scheduletobeassigned_95_equals_to_shift_96 = rail.IfOperator(
            task_id='if_log_scheduletobeassigned_95_equals_to_shift_96',
            test='''{{ result('log_scheduletobeassigned_95') == 'Shift' }}''',
            yes_task="put_schedule_policy_schedule_for_user_97",
            no_task="log_uriof_office_scheduledefault_99",
        )

        put_schedule_policy_schedule_for_user_97 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_97',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": null,
                            "officeSchedule": null,
                            "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_uriof_office_scheduledefault_99 = rail.PythonOperator(
            task_id='log_uriof_office_scheduledefault_99',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_office_schedules_94'), 'displayText', rail.result('log_scheduletobeassigned_95'), 'uri', '')
        )

        if_log_uriof_office_scheduledefault_99_blank_100 = rail.IfOperator(
            task_id='if_log_uriof_office_scheduledefault_99_blank_100',
            test='''{{ result('log_uriof_office_scheduledefault_99') | is_falsy }}''',
            yes_task="log_holiday_calendartobeassigned_104",
            no_task="put_schedule_policy_schedule_for_user_103",
        )

        put_schedule_policy_schedule_for_user_103 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_103',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": "{{ result('log_uriof_office_scheduledefault_99') }}",
                            "name": null,
                            "officeSchedule": null,
                            "scheduleTypeUri": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_holiday_calendartobeassigned_104 = rail.PythonOperator(
            task_id='log_holiday_calendartobeassigned_104',
            python_callable=lambda dag_run: next((x['value'].strip() for x in filter(
                lambda x: x["type"] == "Holiday Calendar" and x["workertype"] == dag_run.conf['workertype'] and x["location"] == dag_run.conf['location'] and (
                    x["exemptstatus"] == dag_run.conf['exemptionstatus']) and x['businesstitle'] == rail.result('log_businesstitle_18'), rail.result(
                        'momentive_userimport_mapper_search_entries_29') or [])), '')
        )

        log_holidaycalendaruri_106 = rail.RepliconServiceOperator(
            task_id='log_holidaycalendaruri_106',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', rail.result('log_holiday_calendartobeassigned_104'), 'uri', '')
        )

        if_log_holidaycalendaruri_106_present_107 = rail.IfOperator(
            task_id='if_log_holidaycalendaruri_106_present_107',
            test='''{{ result('log_holidaycalendaruri_106') | is_truthy }}''',
            yes_task="update_holiday_calendar_for_user_108",
            no_task="log_activitytobeassigned_116",
        )

        update_holiday_calendar_for_user_108 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user_108',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "holidayCalendarUri": "{{ result('log_holidaycalendaruri_106') }}"
            }
        )

        log_activitytobeassigned_116 = rail.PythonOperator(
            task_id='log_activitytobeassigned_116',
            python_callable=lambda dag_run:  next(iter(filter(lambda x: x["type"] == "Activity" and x["workertype"] == dag_run.conf['workertype'] and (
                x["location"] == dag_run.conf['location']) and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and (
                    x['businesstitle'] == rail.result('log_businesstitle_18')), rail.result('momentive_userimport_mapper_search_entries_29'))), '')
        )

        if_log_activitytobeassigned_116_present_117 = rail.IfOperator(
            task_id='if_log_activitytobeassigned_116_present_117',
            test='''{{ result('log_activitytobeassigned_116') | is_truthy }}''',
            yes_task="log_activtiesuri_119",
            no_task="if_request_location_present_121",
        )

        log_activtiesuri_119 = rail.RepliconServiceOperator(
            task_id='log_activtiesuri_119',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', rail.result('log_activitytobeassigned_116')['value'], 'uri', '')
        )

        put_activity_assignments_for_user_120 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_120',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "activityUris": ["{{ result('log_activtiesuri_119') }}"]
            }
        )

        if_request_location_present_121 = rail.IfOperator(
            task_id='if_request_location_present_121',
            test='''{{ dag_run.conf.location | is_truthy }}''',
            yes_task="log_locationfullpath_122",
            no_task="log_timeoff_typestobeassigned_129",
        )

        log_locationfullpath_122 = rail.PythonOperator(
            task_id='log_locationfullpath_122',
            python_callable=lambda dag_run: rail.smartjoin_by_delim(
                str("Momentive / India / " + str(dag_run.conf['location'])).split(" / "), " / ")
        )

        log_locationuri_126 = rail.RepliconServiceOperator(
            task_id='log_locationuri_126',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.search_location_department_group_payload,
            data_handler=lambda response, dag_run: [{
                "uri": item["cells"][0]["uri"],
                "name": item["cells"][0]["textValue"] or null,
                "fullpath": item["cells"][2]['cellCollection'][-1]["textValue"],
                "status": item["cells"][1]["textValue"] if item["cells"][1]["dataType"] == "urn:replicon:list-type:bool" else "False",
            }for item in response['rows'] if item["cells"][0]["textValue"] == dag_run.conf['location']][0]['uri'] if response['rows'] else ''
        )

        if_log_locationuri_126_present_127 = rail.IfOperator(
            task_id='if_log_locationuri_126_present_127',
            test='''{{ result('log_locationuri_126') | is_truthy }}''',
            yes_task="put_policy_data_access_scopes_for_userdepartmentrestricted_128",
            no_task="log_timeoff_typestobeassigned_129",
        )

        put_policy_data_access_scopes_for_userdepartmentrestricted_128 = rail.RepliconServiceOperator(
            task_id='put_policy_data_access_scopes_for_userdepartmentrestricted_128',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "policyDataAccessScopes": [
                    {
                        "policyUri": "urn:replicon:policy:time-off",
                        "locations": [],
                        "divisions": [],
                        "costCenters": [],
                        "serviceCenters": [],
                        "departmentGroups": [
                            {
                                "departmentGroup": {
                                    "uri": "{{ result('log_locationuri_126') }}",
                                    "parentUri": null,
                                    "name": null
                                },
                                "groupSpecificationModeUri": null,
                                "groupDescendantModeUri": null
                            }
                        ],
                        "employeeTypeGroups": []
                    }
                ]
            }
        )

        log_timeoff_typestobeassigned_129 = rail.PythonOperator(
            task_id='log_timeoff_typestobeassigned_129',
            python_callable=lambda dag_run: next(iter(filter(lambda x: x["type"] == "Time off Types" and x["workertype"] == dag_run.conf['workertype'] and
                                                             x["location"] == dag_run.conf['location'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and
                                                             x['businesstitle'] == rail.result('log_businesstitle_18'), rail.result(
                'momentive_userimport_mapper_search_entries_29'))), '')
        )

        def get_exceptions(dag_run):
            exceptions = []
            if bool(rail.result('search_for_user_with_empid') and len(rail.result('search_for_user_with_empid')) > 1):
                exceptions.append("Supervisor not assigned for user " + str(dag_run.conf['firstname'] + str(
                    dag_run.conf['lastname']) + " as multiple users have same Employee ID: " + str(dag_run.conf['managerid']) + "."))

            if rail.result("log_scheduletobeassigned_95") != "Shift" and bool(not (rail.result('log_uriof_office_scheduledefault_99'))):
                exceptions.append("Schedule " + str(rail.result('log_scheduletobeassigned_95')) +
                                  " not found in the instance/ disabled hence schedule not assigned.")

            if bool(not (rail.result('log_get_required_workweek_uri_89'))):
                exceptions.append("Work week " + str(rail.result('log_work_weektobeassigned_87')
                                                     ) + " not found in the instance/disabled hence not assigned")

            if bool(not (rail.result('log_holidaycalendaruri_106'))):
                exceptions.append("Holiday calendar " + str(rail.result('log_holiday_calendartobeassigned_104')
                                                            ) + " not found in the instance hence holiday calendar not assigned.")

            if bool(exceptions):
                return ";".join(exceptions)

            return ""

        log_exceptions = rail.PythonOperator(
            task_id='log_exceptions',
            python_callable=get_exceptions
        )

        if_timeoffs_present_and_active_equal_1 = rail.IfOperator(
            task_id='if_timeoffs_present_and_active_equal_1',
            test="{{ result('log_timeoff_typestobeassigned_129').value | is_truthy and \
                dag_run.conf.active == '1' }}",
            yes_task="trigger_timeoff_add_new_user",
            no_task="momentive_user_import_logs_add_entry_132",
        )

        trigger_timeoff_add_new_user = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_add_new_user',
            trigger_dag_id=config.momentive_india_user_sync_child_add_timeoff_new_user_dag_id,
            conf=request_payload.trigger_timeoff_add_new_user,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_timeoff_add_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_timeoff_add_new_user',
            dag_runs='{{ result("trigger_timeoff_add_new_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_result_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_result_from_child',
            dag_runs='''{{result('trigger_timeoff_add_new_user')}}''',
            dagrun_task_id='catch_error',
            target='result'
        )

        if_error_in_gather_result_from_child = rail.IfOperator(
            task_id='if_error_in_gather_result_from_child',
            test='''{{result('gather_result_from_child') | is_truthy}}''',
            yes_task='stop_processing_due_to_error_in_child',
            no_task='momentive_user_import_logs_add_entry_132'
        )

        stop_processing_due_to_error_in_child = rail.FailOperator(
            task_id='stop_processing_due_to_error_in_child',
            message='''Error in adding timeoff type for new user'''
        )

        momentive_user_import_logs_add_entry_132 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_132',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity=lambda: "Exception" if rail.result(
                'log_exceptions') else "Success",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": rail.render_template("{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"),
                "action": "Add",
                "status": "Exception" if rail.result('log_exceptions') else "Success",
                "details": rail.smartjoin_by_delim(str("User added successfully" + "," + rail.result('log_exceptions')).split(","), ";"),
                "childjobid": get_dagrun_ecid(dag_run),
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            trigger_rule='one_failed',
            severity="Error",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Error",
                "details": rail.render_template("User created, but partially updated ; {{get_error_message()}}") if rail.result(
                    "create_user") else rail.render_template("User not created ;{{ get_error_message() }}"),
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_input_validation_log

        get_input_validation_log >> if_input_validation_log_present

        if_input_validation_log_present >> rail.Label(
            'Yes') >> log_user_import_not_created >> catch_and_log_error
        if_input_validation_log_present >> rail.Label(
            'No') >> if_workertype_not_contingentworker_and_businesstitle_not_present

        if_workertype_not_contingentworker_and_businesstitle_not_present >> rail.Label(
            'Yes') >> log_user_import_not_created_contingent_non_alp >> catch_and_log_error
        if_workertype_not_contingentworker_and_businesstitle_not_present >> rail.Label(
            'No') >> get_all_employee_type

        get_all_employee_type >> get_all_permissionsets >> get_all_timezones >> log_businesstitle_18

        log_businesstitle_18 >> momentive_userimport_mapper_search_entries_19 >> if_mapper_search_entry_present

        if_mapper_search_entry_present >> rail.Label(
            'Yes') >> get_required_employeetype_uri >> if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present
        if_mapper_search_entry_present >> rail.Label(
            'No') >> if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present

        if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present >> rail.Label(
            'Yes') >> details_employeetype_and_departmentygrpuri_not_exist >> log_user_import_employeetype_dept_not_exist >> if_entry_in_mapper_37_value_present_and_deptgrpuri_present
        if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present >> rail.Label(
            'No') >> if_entry_in_mapper_37_value_present_and_deptgrpuri_present

        if_entry_in_mapper_37_value_present_and_deptgrpuri_present >> rail.Label('Yes') >> get_all_pay_rule_scripts_28 \
            >> momentive_userimport_mapper_search_entries_29 >> log_timesheettemplatetobeassigned_30 \
            >> create_payrule_variable >> if_request_location_equals_to_inbangalorembs_32

        if_request_location_equals_to_inbangalorembs_32 >> rail.Label(
            'Yes') >> if_request_india_spec_schedule_indicator_equals_to_yes_33

        if_request_india_spec_schedule_indicator_equals_to_yes_33 >> rail.Label(
            'Yes') >> update_variable_when_india_spec_schedule_indicatorisyesandlocationis_m_b_s_34 >> log_punch_entry_policytobeassigned_39
        if_request_india_spec_schedule_indicator_equals_to_yes_33 >> rail.Label(
            'No') >> update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_m_b_s_36 >> log_punch_entry_policytobeassigned_39

        if_request_location_equals_to_inbangalorembs_32 >> rail.Label(
            'No') >> update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_m_b_s_38 >> log_punch_entry_policytobeassigned_39

        log_punch_entry_policytobeassigned_39 >> log_pay_rule_uri_40 >> create_user >> remove_all_timeoffs >> get_all_policy_sets_46 >> get_required_user_customfields >> if_CF_Date_of_Birth_MM_DD_YYYY_and_dob_uri_present

        if_CF_Date_of_Birth_MM_DD_YYYY_and_dob_uri_present >> rail.Label(
            'Yes') >> if_log_gettherequiredcustomfield_urifor_dateof_birth_49_present_50

        if_log_gettherequiredcustomfield_urifor_dateof_birth_49_present_50 >> rail.Label(
            'Yes') >> update_dob_udf >> if_businesstitle_present
        if_CF_Date_of_Birth_MM_DD_YYYY_and_dob_uri_present >> rail.Label(
            'No') >> if_businesstitle_present

        if_log_gettherequiredcustomfield_urifor_dateof_birth_49_present_50 >> rail.Label(
            'No') >> if_businesstitle_present

        if_businesstitle_present >> rail.Label(
            'Yes') >> if_log_gettherequiredcustomfield_urifor_title_54_present_55

        if_log_gettherequiredcustomfield_urifor_title_54_present_55 >> rail.Label(
            'Yes') >> update_title_udf >> if_fieldhr_present
        if_log_gettherequiredcustomfield_urifor_title_54_present_55 >> rail.Label(
            'No') >> if_fieldhr_present

        if_businesstitle_present >> rail.Label('No') >> if_fieldhr_present

        if_fieldhr_present >> rail.Label(
            'Yes') >> if_log_gettherequiredcustomfield_urifor_h_r_m_58_present_59

        if_log_gettherequiredcustomfield_urifor_h_r_m_58_present_59 >> rail.Label(
            'Yes') >> update_fieldhr_udf >> if_gender_and_genderuri_present
        if_log_gettherequiredcustomfield_urifor_h_r_m_58_present_59 >> rail.Label(
            'No') >> if_gender_and_genderuri_present

        if_fieldhr_present >> rail.Label(
            'No') >> if_gender_and_genderuri_present

        if_gender_and_genderuri_present >> rail.Label(
            'Yes') >> update_gender_udf >> if_manager_id_present
        if_gender_and_genderuri_present >> rail.Label(
            'No') >> if_manager_id_present

        if_manager_id_present >> rail.Label('No') >> get_timesheet_for_date2_85
        if_manager_id_present >> rail.Label(
            'Yes') >> search_for_user_with_empid >> if_login_name_uri_present_71

        if_login_name_uri_present_71 >> rail.Label(
            'Yes') >> get_manager_details >> if_manager_details_present_and_enabled

        if_manager_details_present_and_enabled >> rail.Label(
            'Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permission_not_assigned

        if_supervisor_permission_not_assigned >> rail.Label(
            'Yes') >> add_missing_supervisor_permission >> update_initial_supervisor
        if_supervisor_permission_not_assigned >> rail.Label(
            'No') >> update_initial_supervisor >> get_timesheet_for_date2_85

        if_manager_details_present_and_enabled >> rail.Label(
            'No') >> log_supervisor_assignment >> get_timesheet_for_date2_85

        if_login_name_uri_present_71 >> rail.Label(
            'No') >> log_supervisor_assignment >> get_timesheet_for_date2_85

        get_timesheet_for_date2_85 >> get_timesheet_details_86 >> log_work_weektobeassigned_87 >> get_allworkweek_88 >> log_get_required_workweek_uri_89 >> if_log_get_required_workweek_uri_89_present_90

        if_log_get_required_workweek_uri_89_present_90 >> rail.Label(
            'Yes') >> update_work_week_for_user_91 >> get_all_office_schedules_94
        if_log_get_required_workweek_uri_89_present_90 >> rail.Label(
            'No') >> get_all_office_schedules_94

        get_all_office_schedules_94 >> log_scheduletobeassigned_95 >> if_log_scheduletobeassigned_95_equals_to_shift_96

        if_log_scheduletobeassigned_95_equals_to_shift_96 >> rail.Label(
            'Yes') >> put_schedule_policy_schedule_for_user_97 >> log_holiday_calendartobeassigned_104
        if_log_scheduletobeassigned_95_equals_to_shift_96 >> rail.Label(
            'No') >> log_uriof_office_scheduledefault_99 >> if_log_uriof_office_scheduledefault_99_blank_100

        if_log_uriof_office_scheduledefault_99_blank_100 >> rail.Label(
            'Yes') >> log_holiday_calendartobeassigned_104
        if_log_uriof_office_scheduledefault_99_blank_100 >> rail.Label(
            'No') >> put_schedule_policy_schedule_for_user_103 >> log_holiday_calendartobeassigned_104

        log_holiday_calendartobeassigned_104 >> log_holidaycalendaruri_106 >> if_log_holidaycalendaruri_106_present_107

        if_log_holidaycalendaruri_106_present_107 >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user_108 >> log_activitytobeassigned_116
        if_log_holidaycalendaruri_106_present_107 >> rail.Label(
            'No') >> log_activitytobeassigned_116

        log_activitytobeassigned_116 >> if_log_activitytobeassigned_116_present_117

        if_log_activitytobeassigned_116_present_117 >> rail.Label(
            'Yes') >> log_activtiesuri_119 >> put_activity_assignments_for_user_120 >> if_request_location_present_121
        if_log_activitytobeassigned_116_present_117 >> rail.Label(
            'No') >> if_request_location_present_121

        if_request_location_present_121 >> rail.Label(
            'Yes') >> log_locationfullpath_122 >> log_locationuri_126 >> if_log_locationuri_126_present_127

        if_log_locationuri_126_present_127 >> rail.Label(
            'Yes') >> put_policy_data_access_scopes_for_userdepartmentrestricted_128 >> log_timeoff_typestobeassigned_129
        if_log_locationuri_126_present_127 >> rail.Label(
            'No') >> log_timeoff_typestobeassigned_129

        if_request_location_present_121 >> rail.Label(
            'No') >> log_timeoff_typestobeassigned_129

        log_timeoff_typestobeassigned_129 >> log_exceptions >> if_timeoffs_present_and_active_equal_1

        if_timeoffs_present_and_active_equal_1 >> rail.Label(
            'Yes') >> trigger_timeoff_add_new_user >> wait_for_timeoff_add_new_user >> gather_result_from_child >> if_error_in_gather_result_from_child

        if_error_in_gather_result_from_child >> rail.Label(
            'Yes') >> stop_processing_due_to_error_in_child >> momentive_user_import_logs_add_entry_132
        if_error_in_gather_result_from_child >> rail.Label(
            'No') >> momentive_user_import_logs_add_entry_132

        if_timeoffs_present_and_active_equal_1 >> rail.Label(
            'No') >> momentive_user_import_logs_add_entry_132

        momentive_user_import_logs_add_entry_132 >> catch_and_log_error

        if_entry_in_mapper_37_value_present_and_deptgrpuri_present >> rail.Label(
            'No') >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)

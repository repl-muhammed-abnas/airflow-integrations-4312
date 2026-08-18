import rail
from pwcglobal.user_import_australia import request_payload
from pwcglobal.user_import_australia import custom_methods
from pwcglobal.user_import_australia.tasks.update_user_setting import get_update_user_setting
from pwcglobal.user_import_australia.tasks.update_management_level_udf import create_management_level_task
from pwcglobal.user_import_australia.tasks.assign_supervisor_task import create_assign_supervisor_task
from pwcglobal.user_import_australia.tasks.add_activities_task import create_add_activities_task

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_user_import_australia_user_import_add_new_user_child_{config.instance}",
        description=f"PwCGlobal User Import Australia - User import add new user {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.add_user_max_active_runs
    )as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_config")

        add_new_user = rail.RepliconServiceOperator(
            task_id="add_new_user",
            endpoint="/services/importService1.svc/PutUser2",
            data=request_payload.get_put_user_payload
        )
        user_uri = "{{result('add_new_user').uri}}"
        update_user_setting = get_update_user_setting(user_uri)

        is_user_manager = rail.IfOperator(
            task_id="is_user_manager",
            test="{{dag_run.conf.manager_user == 'Yes'}}",
            yes_task="get_all_permissions",
            no_task="get_enabled_timeoff_types"
        )
        get_all_permissions = rail.RepliconServiceOperator(
            task_id="get_all_permissions",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], "displayText", "AUS - Supervisor", "uri")
        )

        is_aus_supervisor_is_present = rail.IfOperator(
            task_id="is_aus_supervisor_is_present",
            test="{{result('get_all_permissions') != None}}",
            yes_task="add_manager_permissions",
            no_task="get_enabled_timeoff_types"
        )
        add_manager_permissions = rail.RepliconServiceOperator(
            task_id="add_manager_permissions",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{result('add_new_user').uri}}",
                "permissionSetUri": "{{result('get_all_permissions')}}"
            }
        )
        get_enabled_timeoff_types = rail.RepliconServiceOperator(
            task_id="get_enabled_timeoff_types",
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        is_employee_time_type_present = rail.IfOperator(
            task_id="is_employee_time_type_present",
            test="{{dag_run.conf.employee_type | is_truthy and dag_run.conf.time_type | is_truthy}}",
            yes_task="get_all_employee_types",
            no_task="add_user_log"
        )

        get_all_employee_types = rail.RepliconServiceOperator(
            task_id="get_all_employee_types",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            response_filter=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response.json(
            )['d'], 'displayText', dag_run.conf['employee_type']+" - " + dag_run.conf['time_type'])
        )

        is_employee_type_present = rail.IfOperator(
            task_id="is_employee_type_present",
            test="{{result('get_all_employee_types') != None}}",
            yes_task="put_employee_type_schedule",
            no_task="get_all_activities"
        )

        put_employee_type_schedule = rail.RepliconServiceOperator(
            task_id="put_employee_type_schedule",
            endpoint="/services/EmployeeTypeGroupService1.svc/PutEmployeeTypeGroupScheduleForUser",
            data={
                "userUri": "{{result('add_new_user').uri}}",
                "scheduleEntries": [
                    {
                        "employeeTypeGroup": {
                            "uri": "{{result('get_all_employee_types').uri}}",
                            "parent": None,
                            "name": None,
                            "parameterCorrelationId": None
                        },
                        "effectiveDate": None
                    }
                ]
            }
        )

        get_all_activities, get_entries_from_mapper = create_add_activities_task(
            user_uri=None)

        has_any_entries = rail.IfOperator(
            task_id="has_any_entries",
            test="{{result('get_entries_from_mapper') | is_truthy}}",
            yes_task=["get_all_product_available_for_assignment",
                      "get_all_policy_set", "get_all_scripts"],
            no_task="update_completed"
        )

        get_all_product_available_for_assignment = rail.RepliconServiceOperator(
            task_id="get_all_product_available_for_assignment",
            endpoint="/services/AccountManagementService1.svc/GetAllProductsAvailableForUserAssignment",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(response.json(
            )['d'], "displayText", rail.result('get_entries_from_mapper')['Licenses'])
        )
        get_all_policy_set = rail.RepliconServiceOperator(
            task_id="get_all_policy_set",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        get_all_scripts = rail.RepliconServiceOperator(
            task_id="get_all_scripts",
            endpoint="services/PayRuleScriptService2.svc/GetActiveScripts",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(response.json(
            )['d'], "displayText", rail.result("get_entries_from_mapper")['Initialpayrulename'])
        )

        def get_product_payload():
            return {
                "userUri": rail.result('add_new_user')['uri'],
                "productUris": [rail.result('get_all_product_available_for_assignment')['uri']]
            }

        def get_payrule_script_payload():
            return {
                "initialPayRule": {
                    "uri": rail.result('get_all_scripts')['uri']
                },
                "scheduleEntries": []
            } if rail.result('get_all_scripts') else None
        assign_payrule = rail.RepliconServiceOperator(
            task_id="assign_payrule",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda: {
                "user": {
                    "uri": rail.result('add_new_user')['uri']
                },
                "modifications": {
                    "workWeekStartToApply": {
                        "workWeekStartDayUri": "urn:replicon:day-of-week:" +
                        (rail.result('get_entries_from_mapper')[
                         'Workweek'].split(' - ')[0]).lower()
                    },
                    "payRulesToApply": get_payrule_script_payload()
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )
        can_assign_product = rail.IfOperator(
            task_id='can_assign_product',
            test="{{result('get_all_product_available_for_assignment') | is_truthy}}",
            yes_task="assign_product",
            no_task="update_completed"
        )
        assign_product = rail.RepliconServiceOperator(
            task_id="assign_product",
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=get_product_payload
        )

        is_classification_present = rail.IfOperator(
            task_id="is_classification_present",
            test="{{dag_run.conf.classification | is_truthy}}",
            no_task="update_completed",
            yes_task=["get_template_uris_to_assign",
                      "get_all_approval_paths"]
        )

        def get_required_template_uris_to_assign():

            uris = []
            uris.append(rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_set"), "displayText", rail.result(
                'get_entries_from_mapper')['Timesheettemplate'], 'uri'))
            uris.append(rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_set"), "displayText", rail.result(
                'get_entries_from_mapper')['Time off template'], 'uri'))
            uris.append(rail.find_first_by_attr_and_get_attr(rail.result("get_all_policy_set"), "displayText", rail.result(
                'get_entries_from_mapper')['Punchentrypolicy'], 'uri'))
            uris = list(filter(None, uris))
            policy_set_payload = None
            if uris:
                policy_set_payload = {
                    "policySetUrisToAssign": uris,
                    "policyUrisToRemovePolicySet": []
                }
            return{
                "timeoff_type_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_enabled_timeoff_types"),
                                                                         "displayText", rail.result('get_entries_from_mapper')['Time off type']),
                "policy_set_payload": policy_set_payload
            }

        get_template_uris_to_assign = rail.PythonOperator(
            task_id="get_template_uris_to_assign",
            python_callable=get_required_template_uris_to_assign
        )

        has_any_timeoff_type_to_assign = rail.IfOperator(
            task_id="has_any_timeoff_type_to_assign",
            test="{{result('get_template_uris_to_assign').timeoff_type_uri | is_truthy}}",
            no_task="update_completed",
            yes_task="assign_timeoff_type_to_user"
        )
        assign_timeoff_type_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_type_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{result('add_new_user').uri}}",
                "timeOffTypeUris": [
                    "{{result('get_template_uris_to_assign').timeoff_type_uri.uri}}"
                ]
            }
        )
        has_any_policies_to_assign = rail.IfOperator(
            task_id="has_any_policies_to_assign",
            test="{{result('get_template_uris_to_assign').policy_set_payload | is_truthy}}",
            no_task="update_completed",
            yes_task="assign_policies_to_user"
        )

        assign_policies_to_user = rail.RepliconServiceOperator(
            task_id="assign_policies_to_user",
            endpoint="services/ImportService1.svc/ApplyUserModifications2",
            data=lambda: {
                "user": {
                    "uri": rail.result('add_new_user')['uri'],
                },
                "modifications": {
                    "policySetsToApply": rail.result('get_template_uris_to_assign')['policy_set_payload']
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        get_all_approval_paths = rail.RepliconServiceOperator(
            task_id="get_all_approval_paths",
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(response.json(
            )['d'], "displayText", rail.result("get_entries_from_mapper")['Timesheetapprovalpath'])
        )
        is_approval_path_present = rail.IfOperator(
            task_id="is_approval_path_present",
            test="{{result('get_all_approval_paths') | is_truthy}}",
            yes_task="update_approval_path",
            no_task="update_completed"
        )
        update_approval_path = rail.RepliconServiceOperator(
            task_id="update_approval_path",
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{result('add_new_user').uri}}",
                "approvalPathUri": "{{result('get_all_approval_paths').uri}}"
            }
        )

        update_completed = rail.EmptyOperator(
            task_id="update_completed"
        )
        is_supervisor_already_assigned, add_supervisor_end = create_assign_supervisor_task(
            user_uri, caller="add")

        update_user_details = rail.RepliconServiceOperator(
            task_id="update_user_details",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": rail.result('add_new_user')['uri'],
                },
                "modifications": {
                    "timezoneToApply": custom_methods.get_timezone_payload() if dag_run.conf['location_level_2']
                    and rail.result('get_timezone_uri') else None,
                    "holidayCalendarToApply": custom_methods.get_calender_payload() if dag_run.conf['location_level_2']
                    and rail.result('get_all_holiday_calender') else None,
                    "schedulePolicyToApply": custom_methods.get_user_schedule_update_payload() if dag_run.conf['id']
                    and rail.result('get_all_office_schedule') else None,
                    "departmentGroupScheduleToApply": custom_methods.get_department_schedule_update_payload() if dag_run.conf['costcenter_name']
                    and rail.result('search_department_group_by_name') else None,
                    "serviceCenterScheduleToApply": custom_methods.get_service_center_schedule_payload() if dag_run.conf['classification'] else None,
                    "policyDataAccessScopesToApply2": custom_methods.get_data_access_scope_payload() if dag_run.conf['location_level_2']
                    and rail.result('search_location_group2_by_name_code') else None,
                    "locationScheduleToApply": custom_methods.get_add_user_location_update_payload() if dag_run.conf['location_level_1']
                    and rail.result('search_location_group_by_name') else None,
                    "customFieldValuesToApply": custom_methods.get_add_custom_field_payload(dag_run),
                    "payrollRatesToApply": custom_methods.get_payroll_rate_payload() if dag_run.conf['classification'] else None
                }
            }
        )
        is_update_successful = rail.IfOperator(
            task_id="is_update_successful",
            test="{{result('update_user_details').errors | is_truthy}}",
            yes_task="update_user_failed",
            no_task="add_user_log"
        )
        update_user_failed = rail.FailOperator(
            task_id="update_user_failed",
            message="{{result('update_user_details').errors}}"
        )

        get_all_office_schedule = rail.RepliconServiceOperator(
            task_id="get_all_office_schedule",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            response_filter=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], "displayText", dag_run.conf['id'], 'uri')
        )

        get_managementlevel_enabled_dropdown_option, managementlevel_complete = create_management_level_task()

        is_managementlevel_present = rail.IfOperator(
            task_id="is_managementlevel_present",
            test="{{dag_run.conf.management_level | is_truthy}}",
            yes_task=get_managementlevel_enabled_dropdown_option.task_id,
            no_task=managementlevel_complete.task_id
        )

        def search_location_group_by_name_response_filter(response, dag_run):
            response = response.json()['d']
            full_path = custom_methods.get_location_full_path(dag_run, 4)
            # pylint: disable=line-too-long
            return list(filter(lambda x: x['status'] in [True, 'True'] and (x['full_path'] == full_path
                                                                            or x['name'] == dag_run.conf['location_level_4']), map(lambda item: {
                                                                                "uri": item['cells'][0]['uri'],
                                                                                "name": item['cells'][0].get('textValue', ""),
                                                                                "status": item['cells'][1].get('textValue', ""),
                                                                                "full_path":  "/ ".join([x['textValue'] for x in item['cells'][-1]['cellCollection']])
                                                                                if item['cells'][-1]['cellCollection'] else None
                                                                            }, response['rows'])))

        search_location_group_by_name = rail.RepliconServiceOperator(
            task_id="search_location_group_by_name",
            endpoint="/services/LocationListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_search_location_group_by_name_payload(
                dag_run, "location_level_1"),
            response_filter=search_location_group_by_name_response_filter
        )

        get_all_holiday_calender = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calender",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            response_filter=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], "displayText", dag_run.conf['location_level_2'], 'uri')
        )

        get_mapper_timezone_for_location = rail.PythonOperator(
            task_id="get_mapper_timezone_for_location",
            python_callable=custom_methods.get_mapper_timezone_for_location
        )

        def get_timezone(response):
            if rail.result("get_mapper_timezone_for_location"):
                return rail.find_first_by_attr_and_get_attr(response.json()['d'],
                                                            "displayText", rail.result("get_mapper_timezone_for_location")['timezone'], 'uri')
            return None
        get_timezone_uri = rail.RepliconServiceOperator(
            task_id="get_timezone_uri",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
            response_filter=get_timezone
        )

        search_location_group2_by_name_code = rail.RepliconServiceOperator(
            task_id="search_location_group2_by_name_code",
            endpoint="/services/LocationListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_search_location_group_by_name_payload(
                dag_run, "location_level_2"),
            response_filter=lambda response, dag_run: custom_methods.search_location_group2_by_name_code_response_filter(
                response, dag_run, location_index=3)
        )

        search_department_group_by_name = rail.RepliconServiceOperator(
            task_id="search_department_group_by_name",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_search_department_group_by_name_payload,
            response_filter=custom_methods.search_department_group_by_name_response_filter
        )

        get_enabled_currencies = rail.RepliconServiceOperator(
            task_id="get_enabled_currencies",
            endpoint="/services/CurrencyService2.svc/GetEnabledCurrencies",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], "displayText", "AUD$", 'uri')
        )

        def get_cost_center(response, dag_run):
            return list(filter(lambda x: x['name'] == dag_run.conf['classification'], map(
                lambda item: {
                    "name": item['cells'][0].get('textValue', ""),
                    "enabled": item['cells'][1].get('textValue', ""),
                    "uri": item['cells'][0]['uri']
                }, response.json()['d']['rows']
            )))

        search_service_center = rail.RepliconServiceOperator(
            task_id="search_service_center",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data=request_payload.get_search_service_center_payload,
            response_filter=get_cost_center
        )

        get_mapper_classification_records = rail.PythonOperator(
            task_id="get_mapper_classification_records",
            python_callable=custom_methods.get_mapper_classification_records
        )

        def get_exception_logs(dag_run):
            exception_logs = []
            if (dag_run.conf['employee_type'] and dag_run.conf['time_type']) and rail.result('get_all_employee_types') is None:
                exception_logs.append(
                    f"Employee type {dag_run.conf['employee_type']} - {dag_run.conf['time_type']} not found in Replicon")
            if dag_run.conf['location_level_2'] and rail.result('search_location_group2_by_name_code') is None:
                exception_logs.append("Level 3 location not found")
            if dag_run.conf['costcenter_name'] and rail.result('search_department_group_by_name') is None:
                exception_logs.append(
                    f"Company Codes (cost center) not assigned, no company code found for {dag_run.conf['costcenter_name']} in Replicon.")
            if dag_run.conf['classification'] and rail.result('search_service_center') is None:
                exception_logs.append(
                    f"Classification not assigned, no classification found for {dag_run.conf['classification']} in Replicon.")

            return exception_logs

        add_user_log = rail.WriteLogOperator(
            task_id="add_user_log",
            log="{{dag_run.conf.log}}",
            severity=lambda dag_run: "Exception" if get_exception_logs(
                dag_run) else "Success",
            message=lambda dag_run: "User added with exceptions " +
            ",".join(get_exception_logs(dag_run)) if get_exception_logs(
                dag_run) else "User created successfully",
            properties=lambda dag_run: {
                "guid": dag_run.conf['guid'],
                "action": "Add",
                "status": "Exception" if get_exception_logs(dag_run) else "Success",
                "details": "User added with exceptions " + ",".join(get_exception_logs(dag_run))
                if get_exception_logs(dag_run) else "User created successfully",
                "manager_id": "{{dag_run.conf.manager_id}}",
                "processed": "yes"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{dag_run.conf.log}}",
            trigger_rule='one_failed',
            severity='Error',
            message='User partially created ' +
            '{{ get_error_message() }}',
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "action": "Add",
                "status": "Error",
                "details": 'User partially created '+'{{get_error_message()}}',
                "manager_id": "{{dag_run.conf.manager_id}}",
                "processed": "yes"
            },
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        add_new_user >> update_user_setting >> is_user_manager >> rail.Label("Yes") >> get_all_permissions >> rail.Label(
            "Yes") >> is_aus_supervisor_is_present >> rail.Label("Yes") >> add_manager_permissions >> get_enabled_timeoff_types
        is_user_manager >> rail.Label("No") >> get_enabled_timeoff_types

        is_aus_supervisor_is_present >> rail.Label("No") >> get_enabled_timeoff_types >> [is_employee_time_type_present, is_supervisor_already_assigned,
                                                                                          is_managementlevel_present, get_mapper_timezone_for_location]

        is_employee_time_type_present >> rail.Label("Yes") >> get_all_employee_types >> is_employee_type_present >> rail.Label(
            "Yes") >> put_employee_type_schedule >> get_all_activities
        is_employee_type_present >> rail.Label("No") >> get_all_activities

        is_managementlevel_present >> rail.Label(
            "Yes") >> get_managementlevel_enabled_dropdown_option >> managementlevel_complete
        is_managementlevel_present >> rail.Label(
            "No") >> managementlevel_complete

        get_mapper_timezone_for_location >> [get_timezone_uri, get_all_office_schedule, search_location_group_by_name, search_department_group_by_name,
                                             get_enabled_currencies, search_service_center, get_mapper_classification_records,
                                             get_all_holiday_calender, search_location_group2_by_name_code] >> update_user_details >> is_update_successful
        is_update_successful >> rail.Label("Yes") >> add_user_log
        is_update_successful >> rail.Label("No") >> update_user_failed >> rail.Label(
            "On Error") >> catch_and_log_errors

        get_entries_from_mapper >> has_any_entries >> rail.Label("Yes") >> [
            get_all_product_available_for_assignment, get_all_policy_set, get_all_scripts]
        has_any_entries >> rail.Label("No") >> update_completed
        get_all_scripts >> assign_payrule >> update_completed

        get_all_product_available_for_assignment >> can_assign_product >> rail.Label(
            "Yes") >> assign_product >> update_completed
        can_assign_product >> rail.Label("No") >> update_completed

        get_all_policy_set >> is_classification_present >> rail.Label(
            "Yes") >> [get_template_uris_to_assign, get_all_approval_paths]
        get_all_approval_paths >> is_approval_path_present >> rail.Label(
            "Yes") >> update_approval_path >> update_completed

        get_template_uris_to_assign >> [
            has_any_timeoff_type_to_assign, has_any_policies_to_assign]
        has_any_policies_to_assign >> rail.Label(
            "Yes") >> assign_policies_to_user >> update_completed
        has_any_timeoff_type_to_assign >> rail.Label(
            "Yes") >> assign_timeoff_type_to_user >> update_completed

        is_classification_present >> rail.Label("No") >> update_completed
        is_classification_present >> rail.Label("No") >> update_completed
        is_approval_path_present >> rail.Label("No") >> update_completed
        has_any_timeoff_type_to_assign >> rail.Label("No") >> update_completed
        has_any_policies_to_assign >> rail.Label(
            "No") >> update_completed >> add_user_log
        add_supervisor_end >> add_user_log
        managementlevel_complete >> add_user_log
        log_to_sumo << catch_and_log_errors << rail.Label("On error") << add_user_log << rail.Label(
            "No") << is_employee_time_type_present

    return dag


rail.for_each_instance(create_child_dag)

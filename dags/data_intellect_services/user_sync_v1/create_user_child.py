from datetime import timedelta
import json
import rail
from data_intellect_services.user_sync_v1.utils import request_payload
from data_intellect_services.user_sync_v1.utils import response_filters
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid

open_bracket = '{{'
close_bracket = '}}'

null = None

def create_child_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f"data_intellect_user_import_create_user_child_{config.instance}_v1",
        description=f"Data intellect services user sync create user child dag {config.instance} V1",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.create_new_users_child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_create_user_child_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_employee_id_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_employee_id_present',
            end_task='catch_and_log_errors',
        )

        is_employee_id_present = rail.IfOperator(
            task_id='is_employee_id_present',
            test='{{ dag_run.conf.user_details.employee_id | is_truthy }}',
            yes_task='get_named_lists_data_from_hibob',
            no_task='log_employee_id_not_present'
        )

        log_employee_id_not_present = rail.WriteLogOperator(
            task_id='log_employee_id_not_present',
            log='{{ dag_run.conf.log_artifact }}',
            message="Employee Id not present in HIBOB",
            severity='Error',
            properties=lambda dag_run: {
                "username": dag_run.conf["user_details"]["displayname"],
                "employee_id": null,
                "unique_id": dag_run.conf["user_details"]["id"],
                "action": "Create User",
                "status": "Error",
                "comments": "Employee Id not present in HIBOB"
            }
        )

        get_named_lists_data_from_hibob = rail.SimpleHttpOperator(
            task_id='get_named_lists_data_from_hibob',
            method='GET',
            http_conn_id=config.http_conn_id,
            endpoint='company/named-lists',
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            response_filter=lambda response: json.loads(response.text)
        )

        get_human_readable_data_from_hibob = rail.PythonOperator(
            task_id='get_human_readable_data_from_hibob',
            python_callable=response_filters.filter_human_readable_data,
            op_args=[config]
        )

        if_user_already_exists = rail.IfOperator(
            task_id='if_user_already_exists',
            test='{{ dag_run.conf.replicon_user_details | is_truthy }}',
            yes_task='log_user_already_exists',
            no_task='create_user_in_replicon'
        )

        log_user_already_exists = rail.WriteLogOperator(
            task_id='log_user_already_exists',
            log='{{ dag_run.conf.log_artifact }}',
            message="User already present in Replicon",
            severity='Skipped',
            properties=lambda dag_run: {
                "username": dag_run.conf["user_details"]["displayname"],
                "employee_id": dag_run.conf["user_details"]["employee_id"],
                "unique_id": dag_run.conf["user_details"]["id"],
                "action": "Create User",
                "status": "Skipped",
                "comments": "User already present in Replicon"
            }
        )

        create_user_in_replicon = rail.RepliconServiceOperator(
            task_id='create_user_in_replicon',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=request_payload.get_create_user_payload
        )

        declare_var_for_logs = rail.SetVariableOperator(
            task_id='declare_var_for_logs',
            append=False,
            name='exceptions',
            value=[]
        )

        get_holiday_calendar_based_on_location = rail.RepliconServiceOperator(
            task_id='get_holiday_calendar_based_on_location',
            endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
            data_handler=lambda response: list(filter(lambda calendar:
                rail.result("get_human_readable_data_from_hibob")["location"] in calendar['name'], response))
        )

        is_holiday_calendar_present = rail.IfOperator(
            task_id='is_holiday_calendar_present',
            test='{{ result("get_holiday_calendar_based_on_location") | is_truthy }}',
            yes_task='is_user_job_title_present',
            no_task='log_holiday_calendar_not_present'
        )

        log_holiday_calendar_not_present = rail.SetVariableOperator(
            task_id='log_holiday_calendar_not_present',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value={
                "log": "Holiday calendar for location {{ result('get_human_readable_data_from_hibob').location }} not present in Replicon"
            }
        )

        is_user_job_title_present = rail.IfOperator(
            task_id='is_user_job_title_present',
            test='{{ dag_run.conf.user_details.title | is_truthy }}',
            yes_task='get_job_title_customfield_uri',
            no_task='is_user_primary_role_present_in_HIBOB'
        )

        get_job_title_customfield_uri = rail.RepliconServiceOperator(
            task_id='get_job_title_customfield_uri',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={'objectUri': 'urn:replicon:object-type:user'},
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job Title', 'uri'),
        )

        has_job_title_customfield = rail.IfOperator(
            task_id='has_job_title_customfield',
            test='{{ result("get_job_title_customfield_uri") | is_truthy }}',
            yes_task='get_job_title_dropdown_options',
            no_task='log_job_title_custom_field_not_present'
        )

        log_job_title_custom_field_not_present = rail.SetVariableOperator(
            task_id='log_job_title_custom_field_not_present',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value={
                "log": 'Custom Field "Job Title" is not present in Replicon'
            }
        )

        get_job_title_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_job_title_dropdown_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data=lambda: {
                "customFieldUri": rail.result("get_job_title_customfield_uri")
            }
        )

        is_user_job_title_present_in_replicon = rail.IfOperator(
            task_id='is_user_job_title_present_in_replicon',
            test=lambda: rail.find_first_by_attr_and_get_attr(rail.result("get_job_title_dropdown_options"), 'displayText',
                rail.result("get_human_readable_data_from_hibob")["title"], 'uri'),
            yes_task='is_user_primary_role_present_in_HIBOB',
            no_task='create_job_title_dropdown_in_replicon'
        )

        create_job_title_dropdown_in_replicon = rail.RepliconServiceOperator(
            task_id='create_job_title_dropdown_in_replicon',
            endpoint='/services/CustomFieldService1.svc/PutDropDownOptions',
            data=request_payload.get_add_job_title_dropdown_payload
        )

        get_updated_job_title_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_updated_job_title_dropdown_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data=lambda: {
                "customFieldUri": rail.result("get_job_title_customfield_uri")
            }
        )

        is_user_primary_role_present_in_HIBOB = rail.IfOperator(
            task_id='is_user_primary_role_present_in_HIBOB',
            test='{{ result("get_human_readable_data_from_hibob").primary_role | is_truthy }}',
            yes_task='get_user_primary_role_from_replicon',
            no_task='is_department_group_present_in_HIBOB'
        )

        get_user_primary_role_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_primary_role_from_replicon',
            endpoint='/services/ProjectRoleService1.svc/GetAllRoles',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText',
                rail.result("get_human_readable_data_from_hibob")["primary_role"], 'uri')
        )

        is_role_present_in_replicon = rail.IfOperator(
            task_id='is_role_present_in_replicon',
            test='{{ result("get_user_primary_role_from_replicon") | is_truthy }}',
            yes_task='is_department_group_present_in_HIBOB',
            no_task='create_draft_new_role_in_replicon'
        )

        create_draft_new_role_in_replicon = rail.RepliconServiceOperator(
            task_id='create_draft_new_role_in_replicon',
            endpoint='/services/ProjectRoleService1.svc/CreateNewDraft'
        )

        update_role_name = rail.RepliconServiceOperator(
            task_id='update_role_name',
            endpoint='/services/ProjectRoleService1.svc/UpdateName',
            data=request_payload.get_update_role_name_payload
        )

        enable_role = rail.RepliconServiceOperator(
            task_id='enable_role',
            endpoint='/services/ProjectRoleService1.svc/Enable',
            data=request_payload.get_enable_role_payload
        )

        update_isbillable = rail.RepliconServiceOperator(
            task_id='update_isbillable',
            endpoint='/services/ProjectRoleService1.svc/UpdateIsBillable',
            data=request_payload.get_update_isbillable_payload
        )

        publish_draft_new_role = rail.RepliconServiceOperator(
            task_id='publish_draft_new_role',
            endpoint='/services/ProjectRoleService1.svc/PublishDraft',
            data=request_payload.get_publish_draft_new_role
        )

        get_required_currency = rail.RepliconServiceOperator(
            task_id='get_required_currency',
            endpoint='/services/CurrencyService2.svc/GetAllCurrencies',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText',
                "GBP £", 'uri')
        )

        update_cost_rate_on_role = rail.RepliconServiceOperator(
            task_id='update_cost_rate_on_role',
            endpoint='/services/ProjectRoleService1.svc/UpdateCostRateScheduleOverDateRange',
            data=request_payload.get_update_cost_rate_payload
        )

        update_billing_rate_on_role = rail.RepliconServiceOperator(
            task_id='update_billing_rate_on_role',
            endpoint='/services/ProjectRoleService1.svc/UpdateBillingRateScheduleOverDateRange',
            data=request_payload.get_update_billing_rate_payload
        )

        is_department_group_present_in_HIBOB = rail.IfOperator(
            task_id='is_department_group_present_in_HIBOB',
            test='{{ result("get_human_readable_data_from_hibob").team | is_truthy }}',
            yes_task='get_required_department_full_path',
            no_task='is_costcenter_group_present_in_HIBOB'
        )

        get_required_department_full_path = rail.RepliconServiceOperator(
            task_id='get_required_department_full_path',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_required_departments_payload,
            data_handler=response_filters.filter_required_department
        )

        is_department_group_present_in_replicon = rail.IfOperator(
            task_id='is_department_group_present_in_replicon',
            test='{{ result("get_required_department_full_path") | is_truthy }}',
            yes_task='is_costcenter_group_present_in_HIBOB',
            no_task='log_department_group_not_present_in_replicon'
        )

        log_department_group_not_present_in_replicon = rail.SetVariableOperator(
            task_id='log_department_group_not_present_in_replicon',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value={
                "log": 'Team {{result("get_human_readable_data_from_hibob").team}} from HIBOB is not present in Replicon'
            }
        )

        is_costcenter_group_present_in_HIBOB = rail.IfOperator(
            task_id='is_costcenter_group_present_in_HIBOB',
            test='{{ result("get_human_readable_data_from_hibob").cost_center | is_truthy }}',
            yes_task='get_required_costcenter',
            no_task='is_location_present_in_HIBOB'
        )

        get_required_costcenter = rail.RepliconServiceOperator(
            task_id='get_required_costcenter',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, "displayText",
                rail.result("get_human_readable_data_from_hibob")['cost_center'], "uri")
        )

        is_costcenter_group_present_in_replicon = rail.IfOperator(
            task_id='is_costcenter_group_present_in_replicon',
            test='{{ result("get_required_costcenter") | is_truthy }}',
            yes_task='is_location_present_in_HIBOB',
            no_task='log_costcenter_group_not_present'
        )

        log_costcenter_group_not_present = rail.SetVariableOperator(
            task_id='log_costcenter_group_not_present',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value={
                "log": 'Workstream "{{ result("get_human_readable_data_from_hibob").cost_center }}" from HIBOB is not present in Replicon'
            }
        )

        is_location_present_in_HIBOB = rail.IfOperator(
            task_id='is_location_present_in_HIBOB',
            test='{{ result("get_human_readable_data_from_hibob").location | is_truthy }}',
            yes_task='get_required_location',
            no_task='is_employee_type_present_in_HIBOB'
        )

        get_required_location = rail.RepliconServiceOperator(
            task_id='get_required_location',
            endpoint="/services/LocationService1.svc/GetAllLocations",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, "displayText",
                rail.result("get_human_readable_data_from_hibob")['location'], "uri")
        )

        is_location_present_in_replicon = rail.IfOperator(
            task_id='is_location_present_in_replicon',
            test='{{ result("get_required_location") | is_truthy }}',
            yes_task='is_employee_type_present_in_HIBOB',
            no_task='log_location_not_present'
        )

        log_location_not_present = rail.SetVariableOperator(
            task_id='log_location_not_present',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value={
                "log": 'Location "{{ result("get_human_readable_data_from_hibob").location }}" from HIBOB is not present in Replicon'
            }
        )

        is_employee_type_present_in_HIBOB = rail.IfOperator(
            task_id='is_employee_type_present_in_HIBOB',
            test='{{ dag_run.conf.hibob_user_details.payroll.employment.contract | is_truthy }}',
            yes_task='get_required_employee_type',
            no_task='is_supervisor_present_in_HIBOB'
        )

        get_required_employee_type = rail.RepliconServiceOperator(
            task_id='get_required_employee_type',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.get_required_employee_type_payload,
            data_handler=lambda response, dag_run: response_filters.filter_required_employee_type(response, dag_run.conf["user_details"])
        )

        is_employee_type_present_in_replicon = rail.IfOperator(
            task_id='is_employee_type_present_in_replicon',
            test='{{ result("get_required_employee_type") | is_truthy }}',
            yes_task='is_supervisor_present_in_HIBOB',
            no_task='log_employee_type_not_present'
        )

        # pylint: disable=line-too-long
        log_employee_type_not_present = rail.SetVariableOperator(
            task_id='log_employee_type_not_present',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value={
                "log": 'Employee Type "{{ dag_run.conf.hibob_user_details.payroll.employment.type or dag_run.conf.hibob_user_details.payroll.employment.contract }}" from HIBOB is not present in Replicon'
            }
        )

        is_supervisor_present_in_HIBOB = rail.IfOperator(
            task_id='is_supervisor_present_in_HIBOB',
            test='{{ dag_run.conf.user_details.supervisor_emp_id | is_truthy }}',
            yes_task='get_user_supervisor_from_replicon',
            no_task='apply_modifications_on_user_in_replicon'
        )

        get_user_supervisor_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_supervisor_from_replicon',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "employeeId": '{{ dag_run.conf.user_details.supervisor_emp_id }}',
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda res: res[0] if len(
                res) > 0 and res[0]["userDetails"]["uri"] else null
        )

        is_supervisor_present_in_replicon = rail.IfOperator(
            task_id='is_supervisor_present_in_replicon',
            test='{{ result("get_user_supervisor_from_replicon") | is_truthy }}',
            yes_task='apply_modifications_on_user_in_replicon',
            no_task='log_supervisor_not_present_in_replicon'
        )

        log_supervisor_not_present_in_replicon = rail.SetVariableOperator(
            task_id='log_supervisor_not_present_in_replicon',
            append=True,
            name='{{ result("declare_var_for_logs").name }}',
            value={
                "log": 'Supervisor Employee ID "{{ dag_run.conf.user_details.supervisor_emp_id }}" from HIBOB is not present in Replicon'
            }
        )

        apply_modifications_on_user_in_replicon = rail.RepliconServiceOperator(
            task_id='apply_modifications_on_user_in_replicon',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_apply_modifications_user_payload
        )

        get_resource_pool_from_replicon = rail.RepliconServiceOperator(
            task_id='get_resource_pool_from_replicon',
            endpoint='/services/ResourcePoolService1.svc/GetResourcePoolDetails',
            data=request_payload.get_resource_pools_payload
        )

        is_resource_pool_present_in_replicon = rail.IfOperator(
            task_id='is_resource_pool_present_in_replicon',
            test='{{ result("get_resource_pool_from_replicon").uri | is_truthy }}',
            yes_task='assign_resource_pool_to_user',
            no_task='create_resource_pool_in_replicon'
        )

        create_resource_pool_in_replicon = rail.RepliconServiceOperator(
            task_id='create_resource_pool_in_replicon',
            endpoint='/services/ResourcePoolService1.svc/PutResourcePool',
            data=request_payload.get_create_resource_payload
        )

        assign_resource_pool_to_user = rail.RepliconServiceOperator(
            task_id='assign_resource_pool_to_user',
            endpoint='/services/ResourcePoolService1.svc/UpdateUserResourcePoolAssignment',
            data=lambda: request_payload.get_assign_resource_pool_payload(
                rail.result("create_user_in_replicon")["uri"])
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data_handler=lambda response: list(map(lambda timeoff_type_details: timeoff_type_details["uri"], response))
        )

        assign_all_timeoff_types_to_user = rail.RepliconServiceOperator(
            task_id='assign_all_timeoff_types_to_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_in_replicon')['uri'],
                "timeOffTypeUris": rail.result('get_all_time_off_types')
            }
        )

        get_notification_preferences_for_user = rail.RepliconServiceOperator(
            task_id='get_notification_preferences_for_user',
            endpoint="/services/NotificationScriptAdministrationService1.svc/GetUserNotificationPreferences",
            data={
                "userUri": '{{ result("create_user_in_replicon").uri }}'
            }
        )

        put_notification_preferences_for_user = rail.RepliconServiceOperator(
            task_id='put_notification_preferences_for_user',
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data=request_payload.put_notification_pref_payload
        )

        log_exceptions = rail.PythonOperator(
            task_id='log_exceptions',
            python_callable=lambda:  " | ".join(list(map(lambda x: x['log'], rail.get_dag_run_var(rail.result('declare_var_for_logs')[
                                              'name'])))) if rail.get_dag_run_var(rail.result('declare_var_for_logs')['name']) else null
        )

        log_user_created = rail.WriteLogOperator(
            task_id='log_user_created',
            log='{{ dag_run.conf.log_artifact }}',
            message="NA",
            severity='''{{ "Exception" if result('log_exceptions') | is_truthy  else  "Success" }}''',
            properties={
                "username": '{{ dag_run.conf.user_details.displayname }}',
                "employee_id": '{{ dag_run.conf.user_details.employee_id }}',
                "unique_id": '{{ dag_run.conf.user_details.id }}',
                "action": "Create User",
                "status": '''{{ "Exception" if result('log_exceptions') | is_truthy  else  "Success" }}''',
                "comments": '''{{ "User created partially - " + result('log_exceptions') if result('log_exceptions') | is_truthy else "User created successfully"}}'''
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log_artifact }}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                "username": '{{ dag_run.conf.user_details.displayname }}',
                "employee_id": '{{ dag_run.conf.user_details.employee_id }}',
                "unique_id": '{{ dag_run.conf.user_details.id }}',
                "action": "Create User",
                "status": "Error",
                "comments": '{{ get_error_message() }}'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label("No") >> is_employee_id_present
        is_employee_id_present >> rail.Label("Yes") >> get_named_lists_data_from_hibob >> get_human_readable_data_from_hibob >> if_user_already_exists
        is_employee_id_present >> rail.Label("No") >> log_employee_id_not_present >> catch_and_log_errors

        if_user_already_exists >> rail.Label("Yes") >> log_user_already_exists >> catch_and_log_errors
        if_user_already_exists >> rail.Label("No") >> create_user_in_replicon >> declare_var_for_logs \
            >> get_holiday_calendar_based_on_location >> is_holiday_calendar_present
        is_holiday_calendar_present >> rail.Label("Yes") >> is_user_job_title_present
        is_holiday_calendar_present >> rail.Label("No") >> log_holiday_calendar_not_present >> is_user_job_title_present
        is_user_job_title_present >> rail.Label("Yes") >> get_job_title_customfield_uri >> has_job_title_customfield
        is_user_job_title_present >> rail.Label("No") >> is_user_primary_role_present_in_HIBOB
        is_user_job_title_present_in_replicon >> rail.Label("Yes") >> is_user_primary_role_present_in_HIBOB
        is_user_job_title_present_in_replicon >> rail.Label("No") >> create_job_title_dropdown_in_replicon >> get_updated_job_title_dropdown_options \
            >> is_user_primary_role_present_in_HIBOB
        is_user_primary_role_present_in_HIBOB >> rail.Label("Yes") >> get_user_primary_role_from_replicon >> is_role_present_in_replicon
        is_user_primary_role_present_in_HIBOB >> rail.Label("No") >> is_department_group_present_in_HIBOB
        has_job_title_customfield >> rail.Label("Yes") >> get_job_title_dropdown_options \
            >> is_user_job_title_present_in_replicon
        has_job_title_customfield >> rail.Label("No") >> log_job_title_custom_field_not_present >> is_user_primary_role_present_in_HIBOB
        is_role_present_in_replicon >> rail.Label("Yes") >> is_department_group_present_in_HIBOB
        is_role_present_in_replicon >> rail.Label("No") >> create_draft_new_role_in_replicon \
            >> update_role_name >> enable_role >> update_isbillable >> publish_draft_new_role >> get_required_currency >> update_cost_rate_on_role \
                >> update_billing_rate_on_role >> is_department_group_present_in_HIBOB
        is_department_group_present_in_HIBOB >> rail.Label("Yes") >> get_required_department_full_path >> is_department_group_present_in_replicon
        is_department_group_present_in_HIBOB >> rail.Label("No") >> is_costcenter_group_present_in_HIBOB
        is_department_group_present_in_replicon >> rail.Label("No") >> log_department_group_not_present_in_replicon >> is_costcenter_group_present_in_HIBOB
        is_department_group_present_in_replicon >> rail.Label("Yes") >> is_costcenter_group_present_in_HIBOB
        is_costcenter_group_present_in_HIBOB >> rail.Label("Yes") >> get_required_costcenter >> is_costcenter_group_present_in_replicon
        is_costcenter_group_present_in_HIBOB >> rail.Label("No") >> is_location_present_in_HIBOB
        is_costcenter_group_present_in_replicon >> rail.Label("Yes") >> is_location_present_in_HIBOB
        is_costcenter_group_present_in_replicon >> rail.Label("No") >> log_costcenter_group_not_present >> is_location_present_in_HIBOB
        is_location_present_in_HIBOB >> rail.Label("Yes") >> get_required_location >> is_location_present_in_replicon
        is_location_present_in_HIBOB >> rail.Label("No") >> is_employee_type_present_in_HIBOB
        is_location_present_in_replicon >> rail.Label("Yes") >> is_employee_type_present_in_HIBOB
        is_location_present_in_replicon >> rail.Label("No") >> log_location_not_present >> is_employee_type_present_in_HIBOB
        is_employee_type_present_in_HIBOB >> rail.Label("Yes") >> get_required_employee_type >> is_employee_type_present_in_replicon
        is_employee_type_present_in_HIBOB >> rail.Label("No") >> is_supervisor_present_in_HIBOB
        is_employee_type_present_in_replicon >> rail.Label("Yes") >> is_supervisor_present_in_HIBOB
        is_employee_type_present_in_replicon >> rail.Label("No") >> log_employee_type_not_present >> is_supervisor_present_in_HIBOB
        is_supervisor_present_in_HIBOB >> rail.Label("Yes") >> get_user_supervisor_from_replicon >> is_supervisor_present_in_replicon
        is_supervisor_present_in_HIBOB >> rail.Label("No") >> apply_modifications_on_user_in_replicon
        is_supervisor_present_in_replicon >> rail.Label("Yes") >> apply_modifications_on_user_in_replicon
        is_supervisor_present_in_replicon >> rail.Label("No") >> log_supervisor_not_present_in_replicon \
            >> apply_modifications_on_user_in_replicon >> get_resource_pool_from_replicon

        get_resource_pool_from_replicon >> is_resource_pool_present_in_replicon
        is_resource_pool_present_in_replicon >> rail.Label("Yes") >> assign_resource_pool_to_user
        is_resource_pool_present_in_replicon >> rail.Label("No") >> create_resource_pool_in_replicon \
            >> assign_resource_pool_to_user >> get_all_time_off_types >> assign_all_timeoff_types_to_user \
                >> get_notification_preferences_for_user >> put_notification_preferences_for_user >> log_exceptions \
                    >> log_user_created >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)

import rail
from pwcglobal.user_import_australia import request_payload
from pwcglobal.user_import_australia import custom_methods
from pwcglobal.user_import_australia.tasks.update_management_level_udf import create_management_level_task
from pwcglobal.user_import_australia.tasks.assign_supervisor_task import create_assign_supervisor_task


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_user_import_australia_user_import_update_user_child_{config.instance}",
        description=f"PwCGlobal User Import Australia - User import update user {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.update_user_max_active_runs
    )as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_config")
        user_uri = "{{dag_run.conf.user_uri}}"
        can_update_user = rail.IfOperator(
            task_id="can_update",
            test=custom_methods.can_update_user,
            yes_task="get_user_details",
            no_task="log_exception"
        )

        log_exception = rail.WriteLogOperator(
            task_id="log_exception",
            log="{{dag_run.conf.log}}",
            message=lambda dag_run: "User not updated, " +
            custom_methods.get_update_ignore_reason(dag_run),
            severity="Exception",
            properties=lambda dag_run: {
                "guid": dag_run.conf['guid'],
                "action": "update",
                "status": "Exception",
                "details": "User not updated, " + custom_methods.get_update_ignore_reason(dag_run),
                "manager_id": "{{dag_run.conf.manager_id}}",
                "processed": "no"
            }
        )
        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{dag_run.conf.user_uri}}",
                        "loginName": None,
                        "parameterCorrelationId": None
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            }
        )
        is_rehire_user = rail.IfOperator(
            task_id="is_rehire_user",
            test="{{dag_run.conf.active_status == 'Yes' and result('get_user_details')[0].userDetails.isEnabled | is_falsy}}",
            yes_task="can_update_start_date",
            no_task="load_all_data"
        )
        can_update_start_date = rail.IfOperator(
            task_id="can_update_start_date",
            test=lambda dag_run: dag_run.conf['hire_date'] != custom_methods.convert_to_date(
                rail.result('get_user_details')[0]['userDetails']['employmentDateRange']['startDate'], "json"),
            yes_task="update_start_date",
            no_task="enable_login"
        )
        update_start_date = rail.RepliconServiceOperator(
            task_id="update_start_date",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "dateRange": {
                    "startDate": custom_methods.get_payload_format_date(custom_methods.convert_to_date(dag_run.conf['hire_date'], "%d-%m-%Y")),
                    "endDate": None,
                    "relativeDateRangeUri": None,
                    "relativeDateRangeAsOfDate": None
                }
            }
        )
        enable_login = rail.RepliconServiceOperator(
            task_id="enable_login",
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }
        )

        load_all_data = rail.EmptyOperator(
            task_id="load_all_data"
        )

        get_managementlevel_enabled_dropdown_option, managementlevel_complete = create_management_level_task(
            user_uri)
        is_supervisor_already_assigned, add_supervisor_end = create_assign_supervisor_task(
            user_uri, caller="update")

        can_update_managementlevel = rail.IfOperator(
            task_id="can_update_managementlevel",
            test=lambda dag_run: custom_methods.bool_get_can_update(
                dag_run, 'management_level', "Management Level"),
            yes_task=get_managementlevel_enabled_dropdown_option.task_id,
            no_task=managementlevel_complete.task_id
        )
        get_effectivegroup_membership = rail.RepliconServiceOperator(
            task_id="get_effectivegroup_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": user_uri,
                "dateRange": None
            }
        )
        get_timesheet_schedule_for_user = rail.RepliconServiceOperator(
            task_id="get_timesheet_schedule_for_user",
            endpoint="/services/TimesheetPeriodService2.svc/GetTimesheetPeriodScheduleForUser",
            data={
                "userUri": user_uri
            }
        )
        get_all_employee_type_details = rail.RepliconServiceOperator(
            task_id="get_all_employee_type_details",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups"
        )
        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones"
        )
        get_location_uri = rail.RepliconServiceOperator(
            task_id="get_location_uri",
            endpoint="/services/LocationListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_search_location_group_by_name_payload(
                dag_run, location="location_level_1"),
            response_filter=lambda response, dag_run: custom_methods.search_location_group2_by_name_code_response_filter(
                response, dag_run, location_index=4)
        )
        get_classification_uri = rail.RepliconServiceOperator(
            task_id="get_classification_uri",
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
            data=request_payload.get_search_service_center_payload,
            response_filter=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', dag_run.conf['classification'], 'uri')
        )
        get_department_uri = rail.RepliconServiceOperator(
            task_id="get_department_uri",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_search_department_group_by_name_payload,
            response_filter=custom_methods.search_department_group_by_name_response_filter
        )
        get_all_activities = rail.RepliconServiceOperator(
            task_id="get_all_activities",
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",
        )
        get_aus_currency = rail.RepliconServiceOperator(
            task_id="get_aus_currency",
            endpoint="/services/CurrencyService2.svc/GetEnabledCurrencies",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], "displayText", "AUD$", 'uri')
        )
        get_all_scripts = rail.RepliconServiceOperator(
            task_id="get_all_scripts",
            endpoint="services/PayRuleScriptService2.svc/GetActiveScripts",
        )
        get_holiday_calender_uri = rail.RepliconServiceOperator(
            task_id="get_holiday_calender_uri",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            response_filter=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], "displayText", dag_run.conf['location_level_2'], 'uri')
        )

        get_office_schedule_uri = rail.RepliconServiceOperator(
            task_id="get_office_schedule_uri",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            response_filter=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', dag_run.conf['id'], 'uri')
        )
        load_all_data_complete = rail.EmptyOperator(
            task_id="load_all_data_complete"
        )
        get_custom_payload_log_message = rail.PythonOperator(
            task_id="get_custom_payload_log_message",
            python_callable=request_payload.get_user_modification_custom_payload_with_logs
        )
        update_user = rail.RepliconServiceOperator(
            task_id="update_user",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data="{{result('get_custom_payload_log_message').payload | to_json}}"
        )
        is_update_successful = rail.IfOperator(
            task_id="is_update_successful",
            test="{{result('update_user').errors | is_truthy}}",
            yes_task="update_user_failed",
            no_task="can_remove_policies"
        )
        can_remove_policies = rail.IfOperator(
            task_id="can_remove_policies",
            test="{{dag_run.conf.classification | is_falsy or dag_run.conf.classification | starts_with('CPSMA') | is_falsy}}",
            yes_task=["remove_all_users_policies", "remove_timeoff_policies"],
            no_task="log_user_updated"
        )

        update_user_failed = rail.FailOperator(
            task_id="update_user_failed",
            message="{{result('update_user').errors}}"
        )
        remove_all_users_policies = rail.RepliconServiceOperator(
            task_id="remove_all_users_policies",
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data={
                "userUri": "{{dag_run.conf.user_uri}}",
                "policySetUris": []
            }
        )
        remove_timeoff_policies = rail.RepliconServiceOperator(
            task_id="remove_timeoff_policies",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{dag_run.conf.user_uri}}",
                "timeOffTypeUris": []
            }
        )

        log_user_updated = rail.WriteLogOperator(
            task_id="log_user_updated",
            log="{{dag_run.conf.log}}",
            severity="{{result('get_custom_payload_log_message').severity}}",
            message="{{result('get_custom_payload_log_message').log_message}}",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "action": "update",
                "status": "{{result('get_custom_payload_log_message').severity}}",
                "details": "{{result('get_custom_payload_log_message').log_message}}",
                "manager_id": "{{dag_run.conf.manager_id}}",
                "processed": "yes"
            }
        )
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{dag_run.conf.log}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "action": "update",
                "status": "Error",
                "details": 'User partially updated, ' +
                    '{{ get_error_message() }}',
                "manager_id": "{{dag_run.conf.manager_id}}",
                "processed": "yes"
            },
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )
        can_update_user >> rail.Label("No") >> log_exception >> rail.Label(
            "On error") >> catch_and_log_errors >> log_to_sumo
        can_update_user >> rail.Label("Yes") >> get_user_details >> [
            is_rehire_user, can_update_managementlevel, is_supervisor_already_assigned]
        add_supervisor_end >> load_all_data
        [is_rehire_user] >> rail.Label("No") >> load_all_data

        is_rehire_user >> rail.Label("Yes") >> can_update_start_date >> rail.Label(
            "Yes") >> update_start_date >> enable_login >> load_all_data
        can_update_start_date >> rail.Label("No") >> enable_login

        can_update_managementlevel >> rail.Label(
            "Yes") >> get_managementlevel_enabled_dropdown_option >> managementlevel_complete
        can_update_managementlevel >> rail.Label(
            "No") >> managementlevel_complete >> load_all_data

        load_all_data >> [get_effectivegroup_membership, get_timesheet_schedule_for_user, get_all_employee_type_details,
                          get_all_timezones, get_location_uri, get_holiday_calender_uri, get_classification_uri,
                          get_department_uri, get_all_activities, get_aus_currency, get_all_scripts, get_office_schedule_uri] >> load_all_data_complete
        load_all_data_complete >> get_custom_payload_log_message >> update_user >> is_update_successful >> rail.Label("Yes") >> can_remove_policies \
            >> rail.Label("Yes") >> [remove_all_users_policies, remove_timeoff_policies] >> log_user_updated
        is_update_successful >> rail.Label("No") >> update_user_failed >> rail.Label(
            "On error") >> catch_and_log_errors
        can_remove_policies >> rail.Label("No") >> log_user_updated >> rail.Label(
            "On error") >> catch_and_log_errors
        return dag


rail.for_each_instance(create_child_dag)

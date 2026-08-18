from datetime import timedelta
import rail
from guidehouse.workday_user_import.utils import request_payload, response_filters

null = None


def get_user_prereqs_task_group(config):
    """
    Create task group for fetching all user processing prerequisites.

    This task group retrieves all reference data from Replicon that is required
    for creating or updating users. It fetches organizational structures, policy
    configurations, and creates any missing office schedules.

    Args:
        config: Configuration object containing:
            - CUSTOM_FIELDS: List of custom field names to retrieve
            - PERMISSIONS: List of permission set names
            - trigger_parallel_dagrun_count_process_schedules: Parallel schedule processing count
            - process_new_schedule: DAG ID for schedule creation
            - execution_timeout_days: Task execution timeout

    Returns:
        tuple: (entry_task, task_group)
            - entry_task: EmptyOperator to use as task group entry point
            - task_group: TaskGroup object containing all prerequisite tasks
    """

    with rail.TaskGroup(group_id='get_user_prereqs', prefix_group_id=False) as get_user_prereqs:

        dummy_get_user_prereqs = rail.EmptyOperator(
            task_id="dummy_get_user_prereqs"
        )

        def page_handler(request, response):
            if len(response['rows']) > 0:
                request['page'] += 1
                return request
            return None

        get_enabled_divisions = rail.RepliconServicePageOperator(
            task_id='get_enabled_divisions',
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_division_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filters.filter_all_division_data
        )

        get_all_costcenters = rail.RepliconServicePageOperator(
            task_id='get_all_costcenters',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data=request_payload.get_cost_center_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filters.filter_all_costcenters_data
        )

        get_all_servicecenters = rail.RepliconServicePageOperator(
            task_id='get_all_servicecenters',
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data=request_payload.get_service_center_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filters.filter_all_servicecenters_data
        )

        get_location_details = rail.RepliconServicePageOperator(
            task_id='get_location_details',
            endpoint="/services/LocationListService1.svc/GetData",
            data=request_payload.get_location_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filters.filter_all_location_data
        )

        get_employeetype_groups_data = rail.RepliconServicePageOperator(
            task_id='get_employeetype_groups_data',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.get_employeetype_group_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filters.filter_all_employeetype_groups_data
        )

        get_all_timesheet_period_list = rail.RepliconServicePageOperator(
            task_id='get_all_timesheet_period_list',
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            page_handler=page_handler,
            data=request_payload.get_ts_period_payload,
            all_result_data_handler=response_filters.filter_timesheet_period_list
        )

        get_user_customfields = rail.RepliconServiceOperator(
            task_id='get_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': 'urn:replicon:object-type:user'
            },
            data_handler=lambda response: response_filters.get_udf_uris(response, config.CUSTOM_FIELDS)
        )

        get_permission_sets = rail.RepliconServiceOperator(
            task_id='get_permission_sets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: response_filters.get_required_permission(response, config)
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id="get_all_payrule_scripts",
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_timesheet_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_paths',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
        )

        get_timeoff_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timeoff_approval_paths',
            endpoint='/services/TimeOffApprovalService1.svc/GetAllApprovalPaths',
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        get_all_office_schedule = rail.RepliconServiceOperator(
            task_id='get_all_office_schedule',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
        )

        get_all_activity_uris = rail.RepliconServiceOperator(
            task_id='get_all_activity_uris',
            endpoint="/services/ActivityService1.svc/GetAllActivities",
            data_handler=lambda response: list(map(lambda x: {
                "uri": x['uri'],
                "name": x['displayText'],
                "enabled": x['isEnabled'],
            }, response)),
            target="artifact"
        )

        get_dropdowns_values_start = rail.EmptyOperator(
            task_id='get_dropdowns_values_start'
        )

        get_all_user_status_dropdowns = rail.RepliconServiceOperator(
            task_id='get_all_user_status_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_user_customfields').status_uri }}"
            }
        )

        get_dropdowns_values_end = rail.EmptyOperator(
            task_id='get_dropdowns_values_end'
        )

        query_valid_delta_schedules = rail.QueryCollectionOperator(
            task_id='query_valid_delta_schedules',
            name='valid_delta_schedules',
            query="""SELECT DISTINCT schedule FROM valid_data"""
        )

        create_replicon_schedule_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_schedule_collection',
            name="replicon_schedules",
            columns=['displayText', 'slug', 'uri'],
            source="{{ result('get_all_office_schedule') | to_json }}",
        )

        query_schedules_to_add = rail.QueryCollectionOperator(
            task_id='query_schedules_to_add',
            query="""SELECT schedule FROM valid_delta_schedules
                WHERE schedule NOT IN (SELECT DISTINCT displayText FROM replicon_schedules)
                """
        )

        has_new_schedules = rail.IfOperator(
            task_id='has_new_schedules',
            test="{{ result('query_schedules_to_add','length') > 0 }}",
            yes_task='dummy_process_new_schedule',
            no_task='dummy_finish'
        )

        dummy_process_new_schedule = rail.EmptyOperator(
            task_id='dummy_process_new_schedule'
        )

        process_new_schedule = rail.trigger_parallel_dagrun(
            task_id='process_new_schedule',
            items=lambda: rail.result('query_schedules_to_add'),
            parallel_count=config.trigger_parallel_dagrun_count_process_schedules,
            trigger_dag_id=config.process_new_schedule,
            conf={
                "scheduletype": "{{ item.schedule }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        dummy_finish = rail.EmptyOperator(
            task_id="dummy_finish"
        )

        dummy_get_user_prereqs >> [get_enabled_divisions, get_all_costcenters, get_all_servicecenters, get_location_details, get_employeetype_groups_data,
        get_all_timesheet_period_list, get_user_customfields, get_permission_sets, get_all_payrule_scripts, get_all_policy_sets,
        get_timesheet_approval_paths, get_timeoff_approval_paths, get_all_timezones, get_all_office_schedule, get_all_holiday_calendars,
        get_all_activity_uris] >> get_dropdowns_values_start >> [get_all_user_status_dropdowns] >> \
        get_dropdowns_values_end >> query_valid_delta_schedules >> create_replicon_schedule_collection >> query_schedules_to_add >> has_new_schedules >> rail.Label(
            "Yes") >> dummy_process_new_schedule >> process_new_schedule >> dummy_finish
        has_new_schedules >> rail.Label("No") >> dummy_finish

    return dummy_get_user_prereqs, get_user_prereqs

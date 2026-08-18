from ipipeline.user_import.utils import response_filters
import rail
null = None


def get_all_prerequisites_data(config):
    with rail.TaskGroup(
        group_id="get_all_prerequisites_data",
        prefix_group_id=False
    ) as groups_data:

        start_prerequisites = rail.EmptyOperator(task_id="start_prerequisites")

        get_required_holiday_calendars = rail.RepliconServiceOperator(
            task_id="get_required_holiday_calendars",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: response_filters.get_required_holiday_calendars(response, config.assignment_rules_mapper_data)
        )

        get_required_time_off_types = rail.RepliconServiceOperator(
            task_id="get_required_time_off_types",
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response: response_filters.get_required_timeoffs_data(response, config.time_off_type_mapper_data)
        )

        get_required_permission_sets = rail.RepliconServiceOperator(
            task_id="get_required_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: response_filters.get_required_permissions_data(response, config.permissions_mapper_data, config.defaults_mapper_data)
        )

        get_required_timesheet_templates = rail.RepliconServiceOperator(
            task_id="get_required_timesheet_templates",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda response: response_filters.get_required_timesheet_templates_data(response, config.assignment_rules_mapper_data)
        )

        get_required_activities = rail.RepliconServiceOperator(
            task_id="get_required_activities",
            endpoint="/services/ActivityService1.svc/GetAllActivities",
            data_handler=lambda response: response_filters.get_required_activities(response, config.assignment_rules_mapper_data)
        )

        get_required_payrules = rail.RepliconServiceOperator(
            task_id="get_required_payrules",
            endpoint="/services/PayRuleScriptService1.svc/GetAllPayRuleScripts",
            data_handler=lambda response: response_filters.get_required_payrules(response, config.assignment_rules_mapper_data)
        )

        get_all_timesheet_periods = rail.RepliconServiceOperator(
            task_id='get_all_timesheet_periods',
            endpoint='/services/TimesheetPeriodListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:timesheet-period-list-column:timesheet-period",
                    "urn:replicon:timesheet-period-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: response_filters.get_required_timesheet_periods(response, config.assignment_rules_mapper_data)
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response: response_filters.get_all_office_schedules(response, config.assignment_rules_mapper_data)
        )

        get_all_user_oefs = rail.RepliconServiceOperator(
            task_id="get_all_user_oefs",
            endpoint="services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data=lambda: {
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                f'{oef["field_name"]}_oef_uri': rail.find_first_by_attr_and_get_attr(response, 'name', oef['oef_name'], 'uri')
                    for oef in config.oef_field_mapper_data
            }
        )

        end_prerequisites = rail.EmptyOperator(task_id="end_prerequisites")
        
        # Set up the dependencies
        start_prerequisites >> get_required_holiday_calendars >> get_required_time_off_types >> get_required_permission_sets \
            >> get_required_timesheet_templates >> get_required_activities >> get_required_payrules \
                >> get_all_timesheet_periods >> get_all_office_schedules >> get_all_user_oefs >> end_prerequisites

        return groups_data
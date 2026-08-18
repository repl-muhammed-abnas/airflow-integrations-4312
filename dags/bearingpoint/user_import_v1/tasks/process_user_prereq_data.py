from bearingpoint.user_import_v1.utils import custom_methods, request_payload
import rail
null = None


def create_prerequisite_data(config):
    with rail.TaskGroup(
        group_id="create_all_prerequisite_data",
        prefix_group_id=False
    ) as prerequisite_data:

        start_prereq_data = rail.EmptyOperator(task_id="start_prereq_data")

        get_required_object_extension_fields = rail.RepliconServiceOperator(
            task_id="get_required_object_extension_fields",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=custom_methods.get_required_object_extension_fields_data
        )

        get_required_time_off_types = rail.RepliconServiceOperator(
            task_id="get_required_time_off_types",
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response: custom_methods.get_required_timeoffs_data(
                response, config)
        )

        get_required_permission_sets = rail.RepliconServiceOperator(
            task_id="get_required_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=custom_methods.get_required_permission_sets
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calendars",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
        )

        get_required_timesheet_approval_path = rail.RepliconServiceOperator(
            task_id="get_required_timesheet_approval_path",
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
            data_handler=custom_methods.get_required_timesheet_approval_path
        )

        get_required_timeentry_approval_path = rail.RepliconServiceOperator(
            task_id='get_required_timeentry_approval_path',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/GetPageOfApprovalPathsByTextSearch',
            data={
                "page": "1",
                "pageSize": "100000",
                "textSearch": {
                    "queryText": "Project Manager",
                    "searchInDisplayText": "false",
                    "searchInName": "true",
                    "searchInDescription": "false"
                }
            },
            data_handler=custom_methods.get_required_timeentry_approval_path
        )

        get_required_timesheet_periods = rail.RepliconServiceOperator(
            task_id="get_required_timesheet_periods",
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data=request_payload.get_timesheet_period_payload,
            data_handler=custom_methods.get_required_timesheet_periods
        )

        get_required_timesheet_templates = rail.RepliconServiceOperator(
            task_id="get_required_timesheet_templates",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda response, config=config: custom_methods.get_required_timesheet_templates(
                response, config)
        )

        get_all_scheduletypes = rail.RepliconServiceOperator(
            task_id="get_all_scheduletypes",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        end_prereq_data = rail.EmptyOperator(task_id="end_prereq_data")

        start_prereq_data >> get_required_object_extension_fields >> get_required_time_off_types \
            >> get_required_permission_sets >> get_all_holiday_calendars >> get_required_timesheet_approval_path \
                >> get_required_timeentry_approval_path >> get_required_timesheet_periods >> get_required_timesheet_templates \
                    >> get_all_scheduletypes >> end_prereq_data

        return prerequisite_data

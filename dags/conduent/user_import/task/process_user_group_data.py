from conduent.user_import.utils import custom_methods
import rail
null = None


def create_prerequisite_data():
    with rail.TaskGroup(
        group_id="create_all_prerequisite_data",
        prefix_group_id=False
    ):

        start_groups = rail.EmptyOperator(task_id="start_groups")

        get_all_business_groups = rail.RepliconServiceOperator(
            task_id="get_all_business_groups",
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id="get_all_locations",
            endpoint="/services/LocationService1.svc/GetAllLocations"
        )

        get_all_cost_centers = rail.RepliconServiceOperator(
            task_id="get_all_cost_centers",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters"
        )

        get_all_time_zones = rail.RepliconServiceOperator(
            task_id="get_all_time_zones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calendars",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
        )

        get_all_timesheet_approval_path = rail.RepliconServiceOperator(
            task_id="get_all_timesheet_approval_path",
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths"
        )

        get_all_timeoff_approval_path = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_approval_path",
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths"
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                    "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=custom_methods.get_all_custom_fields_data
        )

        end_groups = rail.EmptyOperator(task_id="end_groups")

        start_groups >>\
            [
                get_all_business_groups,
                get_all_locations,
                get_all_cost_centers,
                get_all_time_zones,
                get_all_holiday_calendars,
                get_all_timesheet_approval_path,
                get_all_timeoff_approval_path,
                get_all_office_schedules,
                get_all_custom_fields
            ] >> end_groups
        return start_groups, end_groups

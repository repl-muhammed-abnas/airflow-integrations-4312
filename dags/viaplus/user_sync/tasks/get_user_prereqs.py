"""
ViaPlus User Sync - Get User Prerequisites TaskGroup

This TaskGroup fetches all necessary prerequisite data from Replicon
that will be used for processing users (add, update, disable).
"""
import rail

from viaplus.user_sync.utils import response_filter

null = None


def get_user_prereqs_task_group(config):
    """Create TaskGroup to get all Replicon prerequisites."""

    with rail.TaskGroup(group_id='get_user_prereqs', prefix_group_id=False) as get_user_prereqs:

        dummy_get_user_prereqs = rail.EmptyOperator(
            task_id="dummy_get_user_prereqs"
        )

        # Get all locations
        get_all_locations = rail.RepliconServiceOperator(
            task_id="get_all_locations",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:name",
                    "urn:replicon:location-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_full_path_data
        )

        # Get all departments
        get_all_departments = rail.RepliconServiceOperator(
            task_id="get_all_departments",
            endpoint="/services/DepartmentGroupService1.svc/GetAllDepartmentGroups",
        )

        # Get all legal entities (service centers)
        get_all_legal_entities = rail.RepliconServiceOperator(
            task_id="get_all_legal_entities",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:name",
                    "urn:replicon:cost-center-list-column:code",
                    "urn:replicon:cost-center-list-column:cost-center"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_group_data
        )

        # Get all employee types
        get_all_employee_types = rail.RepliconServiceOperator(
            task_id="get_all_employee_types",
            endpoint="services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:employee-type-group"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_employee_type_data
        )

        # Get all permission sets
        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_default_timeoff_approval_path = rail.RepliconServiceOperator(
            task_id="get_default_timeoff_approval_path",
            endpoint="services/TimeOffApprovalService1.svc/GetApprovalPathForNewUsers",
        )

        # Get all policy sets (templates)
        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        # Get all office schedules
        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        # Get all holiday calendars
        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
        )

        # Get timesheet approval paths
        get_timesheet_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_paths',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
        )

        # Get all timezones
        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        # Get all time off types
        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=response_filter.get_filtered_time_off_types
        )

        # Get all project roles (job titles)
        get_all_project_roles = rail.RepliconServiceOperator(
            task_id="get_all_project_roles",
            endpoint="/services/ProjectRoleService1.svc/GetAllRoles",
        )

        # Get all licenses
        get_all_licenses = rail.RepliconServiceOperator(
            task_id='get_all_licenses',
            endpoint='/services/AccountManagementService1.svc/GetAllProductsAvailableForUserAssignment',
            data_handler=lambda response: response_filter.filter_licenses(response,config)
        )

        # Get user custom fields (UDFs)
        get_user_udfs = rail.RepliconServiceOperator(
            task_id="get_user_udfs",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda udfs: {
                'middle_name_def_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Middle Name', 'uri')
            },
        )

        # Task dependencies - all run in parallel from dummy start
        dummy_get_user_prereqs >> [
            get_all_locations,
            get_all_departments,
            get_all_legal_entities,
            get_all_employee_types,
            get_all_permission_sets,
            get_default_timeoff_approval_path,
            get_all_policy_sets,
            get_all_office_schedules,
            get_all_holiday_calendars,
            get_timesheet_approval_paths,
            get_all_timezones,
            get_all_time_off_types,
            get_all_licenses,
            get_all_project_roles,
            get_user_udfs
        ]

    return dummy_get_user_prereqs, get_user_prereqs

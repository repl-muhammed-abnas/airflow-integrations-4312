from uuid import uuid4
import rail

from mercury_systems_inc.user_import.utils import response_filter

null = None


def pre_requisites_task_group(config):

    with rail.TaskGroup(group_id='get_user_pre_requisites', prefix_group_id=False) as get_user_pre_requisites:

        dummy_get_user_prereqs = rail.EmptyOperator(
            task_id="dummy_get_user_prereqs"
        )

        get_updated_location_grps = rail.RepliconServiceOperator(
            task_id="get_updated_location_grps",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:full-path-code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_full_path_code_data
        )

        get_updated_department_grps = rail.RepliconServiceOperator(
            task_id="get_updated_department_grps",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path-code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_full_path_code_data
        )

        get_all_employee_types_grp = rail.RepliconServiceOperator(
            task_id="get_all_employee_types_grp",
            endpoint="services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:employee-type-group",
                    "urn:replicon:employee-type-group-list-column:full-path-code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_full_path_code_data
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        get_timeoff_policy_starting_balance_set_to_script = rail.RepliconServiceOperator(
            task_id='get_timeoff_policy_starting_balance_set_to_script',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', 'Starting Balance Set To', 'uri')
        )

        get_timeoff_policy_prevent_balance_overdraw_script = rail.RepliconServiceOperator(
            task_id='get_timeoff_policy_prevent_balance_overdraw_script',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', 'Prevent balance overdraw', 'uri')
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=response_filter.get_filtered_replicon_time_off_types
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets'
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars'
        )

        get_all_pay_rules = rail.RepliconServiceOperator(
            task_id='get_all_pay_rules',
            endpoint='/services/PayRuleScriptService1.svc/GetAllPayRuleScripts'
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint='/services/OfficeScheduleService1.svc/GetAllOfficeSchedules'
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
            data_handler=lambda response: list(map(lambda x: {
                'name': x['cells'][0]['textValue'],
                'uri': x['cells'][0]['uri']
            }, response['rows'])) if response['rows'] else []
        )

        get_all_timesheet_approval_paths = rail.RepliconServiceOperator(
            task_id='get_all_timesheet_approval_paths',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths'
        )

        dummy_get_user_prereqs >> [get_updated_location_grps, get_updated_department_grps, get_all_permission_set, get_all_employee_types_grp,
                                   get_all_timezones, get_timeoff_policy_starting_balance_set_to_script,
                                   get_timeoff_policy_prevent_balance_overdraw_script, get_all_time_off_types,
                                   get_all_policy_sets, get_all_holiday_calendars,
                                   get_all_pay_rules, get_all_office_schedules, get_all_timesheet_periods,
                                   get_all_timesheet_approval_paths]

    return dummy_get_user_prereqs, get_user_pre_requisites

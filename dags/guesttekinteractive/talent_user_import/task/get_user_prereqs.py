"""
Get User Prerequisites Task Group - GuestTek Talent User Import Integration

Retrieves all prerequisite data from Replicon needed for user processing.
"""
import rail
from guesttekinteractive.talent_user_import.utils import request_payload, response_filters

null = None


def get_user_prereqs_task_group(config):
    """Create task group for fetching all user processing prerequisites."""
    with rail.TaskGroup(group_id='get_user_prereqs', prefix_group_id=False) as get_user_prereqs:
        
        dummy_start = rail.EmptyOperator(task_id='dummy_get_user_prereqs_start')
        
        def page_handler(request, response):
            if len(response.get('rows', [])) > 0:
                request['page'] += 1
                return request
            return None
        
        get_location_details = rail.RepliconServicePageOperator(
            task_id='get_location_details',
            endpoint="/services/LocationListService1.svc/GetData",
            data=request_payload.get_location_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filters.filter_all_location_data
        )
        
        get_division_details = rail.RepliconServicePageOperator(
            task_id='get_division_details',
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_division_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filters.filter_all_division_data
        )

        get_all_department_groups = rail.RepliconServiceOperator(
            task_id='get_all_department_groups',
            endpoint="/services/DepartmentGroupService1.svc/GetAllDepartmentGroups",
        )
        
        get_employeetype_groups_data = rail.RepliconServicePageOperator(
            task_id='get_employeetype_groups_data',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.get_employeetype_group_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filters.filter_all_employeetype_groups_data
        )
        
        get_user_customfields = rail.RepliconServiceOperator(
            task_id='get_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={'objectUri': 'urn:replicon:object-type:user'},
            data_handler=lambda response: response_filters.get_udf_uris(response, config.CUSTOM_FIELDS)
        )
        
        get_permission_sets = rail.RepliconServiceOperator(
            task_id='get_permission_sets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: response_filters.get_required_permission(response, config)
        )
        
        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )
        
        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )
        
        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
        )
        
        get_all_payrules = rail.RepliconServiceOperator(
            task_id="get_all_payrules",
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )
        
        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )
        
        get_all_timesheet_templates = rail.RepliconServiceOperator(
            task_id='get_all_timesheet_templates',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )
        
        get_all_project_roles = rail.RepliconServiceOperator(
            task_id='get_all_project_roles',
            endpoint="/services/ProjectRoleService1.svc/GetAllRoles",
        )
        
        get_all_service_centers = rail.RepliconServiceOperator(
            task_id='get_all_service_centers',
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
        )
        
        get_all_replicon_users = rail.RepliconServiceOperator(
            task_id='get_all_replicon_users',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={"users": [], "dataLoadOptionUri": "urn:replicon:data-load-option:include-all-users"},
            data_handler=response_filters.filter_replicon_users_for_comparison
        )
        
        dummy_end = rail.EmptyOperator(task_id='dummy_get_user_prereqs_end')
        
        dummy_start >> [
            get_location_details, get_division_details, get_employeetype_groups_data,
            get_user_customfields, get_permission_sets, get_all_timezones,
            get_all_office_schedules, get_all_holiday_calendars, get_all_payrules,
            get_all_time_off_types, get_all_timesheet_templates, get_all_replicon_users,
            get_all_project_roles, get_all_service_centers
        ] >> dummy_end
    
    return dummy_start, get_user_prereqs


def get_updated_user_prereqs_task_group(config):
    """Create lightweight task group for refreshing key prerequisites during processing."""
    with rail.TaskGroup(group_id='get_updated_user_prereqs', prefix_group_id=False) as get_updated_prereqs:
        
        dummy_start = rail.EmptyOperator(task_id="dummy_get_updated_prereqs_start")
        
        def page_handler(request, response):
            if len(response.get('rows', [])) > 0:
                request['page'] += 1
                return request
            return None
        
        get_updated_locations = rail.RepliconServicePageOperator(
            task_id='get_updated_locations',
            endpoint="/services/LocationListService1.svc/GetData",
            data=request_payload.get_location_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filters.filter_all_location_data
        )
        
        get_updated_employeetypes = rail.RepliconServicePageOperator(
            task_id='get_updated_employeetypes',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.get_employeetype_group_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filters.filter_all_employeetype_groups_data
        )
        
        get_updated_schedules = rail.RepliconServiceOperator(
            task_id='get_updated_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )
        
        dummy_end = rail.EmptyOperator(task_id="dummy_get_updated_prereqs_end")
        
        dummy_start >> [get_updated_locations, get_updated_employeetypes, get_updated_schedules] >> dummy_end
    
    return dummy_start, get_updated_prereqs

import rail
from galaxyusopcoinc.workday_user_sync.user_import_v2.utils import request_payload, response_filter
null = None


def gather_required_details():
    with rail.TaskGroup(group_id='gather_required_details', prefix_group_id=False):

        gather_required_details_start = rail.EmptyOperator(
            task_id='gather_required_details_start'
        )
        query_parent_department = rail.QueryCollectionOperator(
            task_id='query_parent_department',
            query="SELECT DISTINCT JobFamilyGroup FROM queryuserimportdata WHERE NULLIF(JobFamilyGroup, '') IS NOT NULL GROUP BY JobFamilyGroup"
        )

        query_child_department = rail.QueryCollectionOperator(
            task_id='query_child_department',
            query="SELECT DISTINCT JobFamily,JobFamilyGroup FROM queryuserimportdata WHERE NULLIF(JobFamily, '') IS NOT NULL GROUP BY JobFamily"
        )

        query_parent_locations = rail.QueryCollectionOperator(
            task_id='query_parent_locations',
            query="SELECT DISTINCT Country FROM queryuserimportdata"
        )

        query_child_locations = rail.QueryCollectionOperator(
            task_id='query_child_locations',
            query="SELECT DISTINCT Country, Location FROM queryuserimportdata WHERE NULLIF(Location,'') IS NOT NULL GROUP BY Location"
        )

        query_distinct_costcenter = rail.QueryCollectionOperator(
            task_id='query_distinct_costcenter',
            query="SELECT DISTINCT CostCenterName,CostCenterID FROM queryuserimportdata"
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint='/services/LocationService1.svc/GetAllLocations',
        )

        get_all_costcenter = rail.RepliconServiceOperator(
            task_id='get_all_costcenter',
            endpoint='/services/CostCenterListService1.svc/GetData',
            data=request_payload.get_costcenter_payload,
            response_filter=response_filter.map_list_data
        )

        get_enabled_emp_groups = rail.RepliconServiceOperator(
            task_id='get_enabled_emp_groups',
            endpoint='/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups',
        )

        get_dept_group = rail.RepliconServiceOperator(
            task_id='get_dept_group',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data=request_payload.get_dept_group_payload,
            response_filter=response_filter.map_list_data
        )

        get_all_timeofftypes = rail.RepliconServiceOperator(
            task_id='get_all_timeofftypes',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            response_filter=response_filter.map_response_data
        )

        get_all_permissionset = rail.RepliconServiceOperator(
            task_id='get_all_permissionset',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
        )

        get_all_approval_paths = rail.RepliconServiceOperator(
            task_id='get_all_approval_paths',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
        )

        get_timeentry_apprroval_paths = rail.RepliconServiceOperator(
            task_id='get_timeentry_apprroval_paths',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/GetPageOfApprovalPathsByTextSearch',
            data={"page": "1", "pageSize": "1000", "textSearch": null},
            response_filter=response_filter.map_response_data
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
        )

        get_all_polices = rail.RepliconServiceOperator(
            task_id="get_all_polices",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id='get_all_timezones',
            endpoint='/services/InternationalizationService1.svc/GetAllTimeZones',
        )

        get_all_ObjectExtensionfields = rail.RepliconServiceOperator(
            task_id='get_all_ObjectExtensionfields',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={'bindingContextUri': 'urn:replicon:object-type:user'},
        )

        get_user_custom_fields_group = rail.RepliconServiceOperator(
            task_id='get_user_custom_fields_group',
            endpoint='/services/CustomFieldService1.svc/GetCustomFieldGroup',
            data={"objectTypeUri": "urn:replicon:object-type:user"},
        )

        get_user_new_employee_custom_field = rail.RepliconServiceOperator(
            task_id='get_user_new_employee_custom_field',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                "objectUri": "{{result('get_user_custom_fields_group').uri}}"},
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, "displayText", "New Employee")
        )

        gather_required_details_done = rail.EmptyOperator(
            task_id='gather_required_details_done'
        )

        gather_required_details_start >> [get_all_ObjectExtensionfields, get_all_locations, get_all_approval_paths, get_all_polices,
                                          get_enabled_emp_groups, get_dept_group, get_all_costcenter, get_all_holiday_calendars,
                                          get_all_permissionset, query_child_locations, query_distinct_costcenter,
                                          get_timeentry_apprroval_paths, get_all_timeofftypes, get_all_timezones, query_parent_department,
                                          query_parent_locations, query_child_department] >> get_user_custom_fields_group >> get_user_new_employee_custom_field\
            >> gather_required_details_done

        return (gather_required_details_start, gather_required_details_done)

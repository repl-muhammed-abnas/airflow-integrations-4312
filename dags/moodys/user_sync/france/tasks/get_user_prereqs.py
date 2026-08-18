import rail

from moodys.user_sync.france.utils import request_payload, response_filter

null = None

def get_user_prereqs_task_group():

    with rail.TaskGroup(group_id='get_user_prereqs', prefix_group_id=False) as get_user_prereqs:

        dummy_get_user_prereqs = rail.EmptyOperator(
            task_id="dummy_get_user_prereqs"
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint='/services/LocationListService1.svc/GetData',
            data=request_payload.get_location_payload,
            data_handler= response_filter.filter_group_data
        )

        get_all_departments = rail.RepliconServiceOperator(
            task_id='get_all_departments',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data=request_payload.get_dept_group_payload,
            data_handler=response_filter.filter_group_data
        )

        get_all_divisions = rail.RepliconServiceOperator(
            task_id="get_all_divisions",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:division-list-column:name",
                        "urn:replicon:division-list-column:division"
                    ]
            },
            data_handler=response_filter.filter_divisions_data
        )

        get_all_employeetypes = rail.RepliconServiceOperator(
            task_id="get_all_employeetypes",
            endpoint="services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.get_all_employee_grp_payload,
            data_handler=response_filter.filter_group_data
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id="get_all_payrule_scripts",
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        get_timesheet_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_paths',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
        )

        get_timeoff_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timeoff_approval_paths',
            endpoint='/services/TimeOffApprovalService1.svc/GetAllApprovalPaths',
        )

        get_required_time_off_types = rail.RepliconServiceOperator(
            task_id='get_required_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=response_filter.get_required_time_off_types
        )

        get_user_udfs = rail.RepliconServiceOperator(
            task_id="get_user_udfs",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda udfs: {
                'customfieldgroupuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Regular/Shift User', 'group.uri'),
                'regularshiftuserdefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Regular/Shift User', 'uri'),
                'ftepercentdefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'FTE%', 'uri'),
                'employeecategorydefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Employee Category', 'uri'),
                'employeecategory2definitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Employee Category 2', 'uri'),
                'rehiredefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Rehire', 'uri'),
                'adpfiledefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'ADP File#', 'uri'),
                'actualworkinghrsdefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Actual Working Hours', 'uri'),
                'locationudfdefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Location', 'uri')
            },
        )

        get_regularshiftuser_udf_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_regularshiftuser_udf_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['regularshiftuserdefinitionuri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        get_employeecategory_udf_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_employeecategory_udf_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['employeecategorydefinitionuri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        dummy_get_user_prereqs >> [get_all_locations, get_all_departments, get_all_divisions, get_all_employeetypes, get_all_permission_set,
            get_all_payrule_scripts, get_all_policy_sets,get_all_timezones,
            get_timesheet_approval_paths, get_timeoff_approval_paths, get_user_udfs, get_required_time_off_types]

        get_user_udfs >> [get_regularshiftuser_udf_dropdown_values, get_employeecategory_udf_dropdown_values]

    return dummy_get_user_prereqs, get_user_prereqs

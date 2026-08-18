import rail

from ttecholdingsinc.user_sync.utils.request_payload import get_location_payload
from ttecholdingsinc.user_sync.utils.response_filter import filter_group_data, groups_filter, get_all_drop_down_options_filter

null = None


def get_user_prereqs_task_group():

    with rail.TaskGroup(group_id='get_user_prereqs', prefix_group_id=False) as get_user_prereqs:

        dummy_get_user_prereqs = rail.EmptyOperator(
            task_id="dummy_get_user_prereqs"
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint='/services/LocationListService1.svc/GetData',
            data=get_location_payload,
            data_handler=filter_group_data
        )

        get_updated_departments = rail.RepliconServiceOperator(
            task_id="get_updated_departments",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:name",
                    "urn:replicon:department-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=groups_filter
        )

        get_all_employee_types = rail.RepliconServiceOperator(
            task_id="get_all_employee_types",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:name",
                    "urn:replicon:employee-type-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=groups_filter
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id="get_all_payrule_scripts",
            endpoint="/services/PayRuleScriptService2.svc/GetActiveScripts",
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

        get_user_udfs = rail.RepliconServiceOperator(
            task_id="get_user_udfs",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda udfs: {
                'person_type_definitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'PERSON_TYPE', 'uri'),
                'job_title_definitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Job Title', 'uri'),
                'job_code_definitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Job Code', 'uri'),
                'tax_id_definitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'AFM', 'uri'),
                'expected_weekly_hrs_definitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Expected Weekly Hours', 'uri')
            },
        )

        get_person_type_udf_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_person_type_udf_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                    "customFieldUri": rail.result('get_user_udfs')['person_type_definitionuri']
            },
            data_handler=get_all_drop_down_options_filter
        )

        dummy_get_user_prereqs >> [get_all_locations, get_updated_departments, get_all_employee_types, get_all_permission_set, get_timeoff_approval_paths,
                                   get_all_payrule_scripts, get_all_policy_sets, get_all_timezones, get_timesheet_approval_paths, get_user_udfs]

        get_user_udfs >> get_person_type_udf_dropdown_values

    return dummy_get_user_prereqs, get_user_prereqs

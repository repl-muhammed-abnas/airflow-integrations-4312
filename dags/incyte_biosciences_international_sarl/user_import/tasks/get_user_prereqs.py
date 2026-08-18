import rail

from incyte_biosciences_international_sarl.user_import.utils import request_payload, response_filter
from incyte_biosciences_international_sarl.user_import.utils.python_callable_methods import create_hr_manager_udf_add_payload

null = None

def get_user_prereqs_task_group():

    with rail.TaskGroup(group_id='get_user_prereqs', prefix_group_id=False) as get_user_prereqs:

        dummy_get_user_prereqs = rail.EmptyOperator(
            task_id="dummy_get_user_prereqs"
        )

        get_updated_countries_grp = rail.RepliconServiceOperator(
            task_id='get_updated_countries_grp',
            endpoint='/services/LocationListService1.svc/GetData',
            data=request_payload.get_location_payload,
            data_handler= response_filter.filter_group_data
        )

        get_updated_work_location_grp = rail.RepliconServiceOperator(
            task_id="get_updated_work_location_grp",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:division-list-column:division"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_group_data
        )

        get_updated_standard_hours_grp = rail.RepliconServiceOperator(
            task_id="get_updated_standard_hours_grp",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:service-center"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_group_data
        )

        get_updated_full_part_time_grp = rail.RepliconServiceOperator(
            task_id="get_updated_full_part_time_grp",
            endpoint='/services/CostCenterListService1.svc/GetData',
            data=request_payload.get_costcenter_payload,
            data_handler=response_filter.filter_group_data
        )

        get_updated_departments_grp = rail.RepliconServiceOperator(
            task_id="get_updated_departments_grp",
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
            data_handler=response_filter.filter_departments_data
        )

        get_updated_employee_types_grp = rail.RepliconServiceOperator(
            task_id="get_updated_employee_types_grp",
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

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        get_all_holiday_calenders = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calenders',
            endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
        )

        get_timesheet_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_paths',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
        )

        get_user_udfs = rail.RepliconServiceOperator(
            task_id="get_user_udfs",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda udfs: {
                'business_title_definition_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Business Title', 'uri'),
                'fte_definition_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'FTE%', 'uri'),
                'hr_manager_id_definition_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'HR Manager', 'uri')
            },
        )

        get_hr_manager_udf_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_hr_manager_udf_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['hr_manager_id_definition_uri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        create_hr_manager_udf_collection_replicon = rail.CreateCollectionOperator(
            task_id="create_hr_manager_udf_collection_replicon",
            columns=['name', 'uri'],
            name="replicon_hr_manager",
            source="{{ result('get_hr_manager_udf_dropdown_values') | to_json }}"
        )

        query_hr_manager_udf_values_add = rail.QueryCollectionOperator(
            task_id="query_hr_manager_udf_values_add",
            query="""SELECT DISTINCT hr_manager_id FROM valid_records WHERE NULLIF(hr_manager_id,"") is NOT NULL and LOWER(hr_manager_id) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_hr_manager)""",
            name='new_hr_manager_udf_values'
        )

        has_any_hr_manager_udf_values_to_add = rail.IfOperator(
            task_id="has_any_hr_manager_udf_values_to_add",
            test="{{result('query_hr_manager_udf_values_add', 'length') > 0}}",
            yes_task="create_hr_manager_add_payload",
            no_task="get_updated_hr_manager_udf_dropdown_values"
        )

        create_hr_manager_add_payload = rail.PythonOperator(
            task_id="create_hr_manager_add_payload",
            python_callable=create_hr_manager_udf_add_payload
        )

        put_hr_manager_dropdown_values = rail.RepliconServiceOperator(
            task_id="put_hr_manager_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_user_udfs')['hr_manager_id_definition_uri'],
                "customFieldDropDownOptionUris": rail.result('create_hr_manager_add_payload')
            }
        )

        get_updated_hr_manager_udf_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_updated_hr_manager_udf_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['hr_manager_id_definition_uri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        dummy_get_user_prereqs >> [get_updated_countries_grp, get_updated_standard_hours_grp, get_updated_full_part_time_grp, get_updated_departments_grp,
            get_updated_work_location_grp, get_all_office_schedules,get_updated_employee_types_grp, get_all_holiday_calenders, get_timesheet_approval_paths,
            get_all_permission_set, get_all_payrule_scripts, get_all_policy_sets, get_user_udfs]

        get_user_udfs >> get_hr_manager_udf_dropdown_values >> create_hr_manager_udf_collection_replicon >> query_hr_manager_udf_values_add
        query_hr_manager_udf_values_add >> has_any_hr_manager_udf_values_to_add >> rail.Label('No') >> get_updated_hr_manager_udf_dropdown_values
        has_any_hr_manager_udf_values_to_add >> rail.Label('Yes') >> create_hr_manager_add_payload >> put_hr_manager_dropdown_values
        put_hr_manager_dropdown_values >> get_updated_hr_manager_udf_dropdown_values


    return dummy_get_user_prereqs, get_user_prereqs

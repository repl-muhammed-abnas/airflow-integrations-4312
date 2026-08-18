import rail

from lanter_delivery_systems.user_import.user_import_integration.utils.request_payload import get_location_payload
from lanter_delivery_systems.user_import.user_import_integration.utils.response_filter import filter_group_data, groups_filter,\
get_all_drop_down_options_filter, filter_product_license_description

null = None

def get_user_prereqs_task_group():

    with rail.TaskGroup(group_id='get_user_prereqs', prefix_group_id=False) as get_user_prereqs:

        dummy_get_user_prereqs = rail.EmptyOperator(
            task_id="dummy_get_user_prereqs"
        )

        get_updated_locations = rail.RepliconServiceOperator(
            task_id='get_updated_locations',
            endpoint='/services/LocationListService1.svc/GetData',
            data=get_location_payload,
            data_handler= filter_group_data
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

        get_updated_employee_types = rail.RepliconServiceOperator(
            task_id="get_updated_employee_types",
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

        get_all_products_available_for_user_assignment=rail.RepliconServiceOperator(
            task_id='get_all_products_available_for_user_assignment',
            endpoint="/services/AccountManagementService1.svc/GetAllProductsAvailableForUserAssignment",
            data_handler=filter_product_license_description
        )

        get_all_currencies=rail.RepliconServiceOperator(
            task_id='get_all_currencies',
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
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
                'districtdefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'District', 'uri'),
                'costcenterdefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Cost Center', 'uri'),
                'ciddefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'CID', 'uri'),
                'locationaddressdefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Location Address Line 1', 'uri'),
                'locationcitydefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Location City', 'uri'),
                'locationstatedefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Location State/Territory', 'uri'),
                'accountingcodedefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'accounting_code:gl_string', 'uri'),
                'accountingcodedescriptionfinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'accounting_code:gl_description', 'uri'),
                'worktypedefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'work_type', 'uri'),
                'agencydefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Agency', 'uri'),
                'markupdefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'markup %', 'uri'),
                'glstringdefinitionuri':  rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'GL String', 'uri'),
            },
        )

        get_work_type_udf_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_work_type_udf_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['worktypedefinitionuri']
                },
            data_handler=get_all_drop_down_options_filter
        )

        dummy_get_user_prereqs >> [get_updated_locations, get_updated_departments, get_updated_employee_types, get_all_permission_set,
            get_all_payrule_scripts, get_all_policy_sets,get_all_timezones,get_all_products_available_for_user_assignment, get_all_currencies,
            get_timesheet_approval_paths,get_user_udfs]

        get_user_udfs >> get_work_type_udf_dropdown_values

    return dummy_get_user_prereqs, get_user_prereqs

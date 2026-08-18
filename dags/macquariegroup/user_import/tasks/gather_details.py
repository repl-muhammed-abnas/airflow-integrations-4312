import rail
from macquariegroup.user_import.utils import data_handlers
null = None


def get_gather_details_task():
    with rail.TaskGroup(group_id="gather_details", prefix_group_id=False) as gather_details_task:

        get_all_departments = rail.RepliconServiceOperator(
            task_id="get_all_departments",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:name",
                    "urn:replicon:department-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=data_handlers.get_all_department_filter
        )

        get_all_cost_centers = rail.RepliconServiceOperator(
            task_id="get_all_cost_centers",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data_handler=data_handlers.get_all_cost_centers_filter
        )

        get_all_timesheet_period = rail.RepliconServiceOperator(
            task_id="get_all_timesheet_period",
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:timesheet-period-list-column:enabled",
                        "urn:replicon:timesheet-period-list-column:timesheet-period"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=data_handlers.get_all_timesheet_period_filter
        )

        get_user_custom_field_group = rail.RepliconServiceOperator(
            task_id="get_user_custom_field_group",
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroup",
            data={
                "objectTypeUri": "urn:replicon:object-type:user"
            }
        )

        get_all_user_oef_fields = rail.RepliconServiceOperator(
            task_id="get_all_user_oef_fields",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=data_handlers.get_all_user_oef_fields_filter
        )

        get_all_user_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_user_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{result('get_user_custom_field_group').uri}}"
            },
            data_handler=data_handlers.get_all_user_custom_fields_filter
        )

        [get_all_departments, get_all_cost_centers,
            get_all_timesheet_period, get_user_custom_field_group, get_all_user_oef_fields] >> get_all_user_custom_fields

        return gather_details_task

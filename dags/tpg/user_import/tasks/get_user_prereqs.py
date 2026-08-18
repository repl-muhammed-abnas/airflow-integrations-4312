import rail
from tpg.user_import.utils.request_payload import get_location_payload
from tpg.user_import.utils.response_filter import filter_group_data, groups_filter

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

        get_updated_buisness_unit_grps = rail.RepliconServiceOperator(
            task_id="get_updated_buisness_unit_grps",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:division-list-column:name",
                    "urn:replicon:division-list-column:full-path"
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

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_timesheet_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_paths',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
        )

        get_default_office_schedule = rail.RepliconServiceOperator(
            task_id = 'get_default_office_schedule',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        def filter_timesheet_period_list(response):
            return list(map(lambda row:
                {
                    "uri": row["cells"][0]["uri"],
                    "name": row["cells"][1].get('textValue')
                }, response["rows"]))

        get_all_timesheet_period_list = rail.RepliconServiceOperator(
            task_id='get_all_timesheet_period_list',
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:timesheet-period-list-column:timesheet-period",
                    "urn:replicon:timesheet-period-list-column:name"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=filter_timesheet_period_list
        )

        get_user_udfs = rail.RepliconServiceOperator(
            task_id="get_user_udfs",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda udfs: {
                'leveluri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Level', 'uri')
            },
        )

        dummy_get_user_prereqs >> [get_updated_locations, get_updated_departments, get_updated_employee_types, get_updated_buisness_unit_grps,
            get_all_permission_set,get_all_policy_sets,get_timesheet_approval_paths,get_default_office_schedule,get_all_timesheet_period_list,get_user_udfs]


    return dummy_get_user_prereqs, get_user_prereqs

from ipipeline.user_import.utils import response_filters
import rail
null = None


def get_all_groups_data(type_of_data):
    with rail.TaskGroup(
        group_id=f"{type_of_data}_groups_data",
        prefix_group_id=False
    ) as groups_data:

        start_groups = rail.EmptyOperator(task_id=f"start_{type_of_data}_groups")

        get_all_location_groups_data = rail.RepliconServiceOperator(
            task_id=f"get_{type_of_data}_location_groups_data",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 100000,
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:full-path",
                    "urn:replicon:location-list-column:full-path-code",
                    "urn:replicon:location-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filters.get_existing_details_of_group,
            target='artifact'
        )

        get_all_department_groups_data = rail.RepliconServiceOperator(
            task_id=f"get_{type_of_data}_department_groups_data",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 100000,
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path",
                    "urn:replicon:department-group-list-column:full-path-code",
                    "urn:replicon:department-group-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filters.get_existing_details_of_group,
            target='artifact'
        )

        get_all_employeetype_groups_data = rail.RepliconServiceOperator(
            task_id=f"get_{type_of_data}_employeetype_groups_data",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 100000,
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:employee-type-group",
                    "urn:replicon:employee-type-group-list-column:full-path",
                    "urn:replicon:employee-type-group-list-column:full-path-code",
                    "urn:replicon:employee-type-group-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filters.get_existing_details_of_group,
            target='artifact'
        )

        get_all_servicecenter_groups_data = rail.RepliconServiceOperator(
            task_id=f"get_{type_of_data}_servicecenter_groups_data",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 100000,
                "columnUris": [
                    "urn:replicon:service-center-list-column:service-center",
                    "urn:replicon:service-center-list-column:full-path",
                    "urn:replicon:service-center-list-column:full-path-code",
                    "urn:replicon:service-center-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filters.get_existing_details_of_group,
            target='artifact'
        )

        get_all_project_roles = rail.RepliconServiceOperator(
            task_id=f"get_{type_of_data}_project_roles",
            endpoint="/services/ProjectRoleListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 100000,
                "columnUris":  [
                    "urn:replicon:project-role-list-column:project-role",
                    "urn:replicon:project-role-list-column:cost",
                    "urn:replicon:project-role-list-column:current-billing-rate"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filters.get_project_roles_data,
            target='artifact'
        )

        end_groups = rail.EmptyOperator(task_id=f"end_{type_of_data}_groups")

        start_groups >> get_all_location_groups_data >> get_all_department_groups_data >> get_all_employeetype_groups_data >> get_all_servicecenter_groups_data >> get_all_project_roles >> end_groups

        return groups_data
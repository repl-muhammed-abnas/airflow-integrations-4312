from tsystems.user_import_v1.utils import response_filters
import rail
null = None


def get_all_groups_data():
    with rail.TaskGroup(
        group_id="get_all_groups_data",
        prefix_group_id=False
    ) as groups_data:

        start_groups = rail.EmptyOperator(task_id="start_groups")

        get_location_group_as_orgstructure = rail.RepliconServiceOperator(
            task_id="get_location_group_as_orgstructure",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 10000,
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

        get_servicecenter_as_department = rail.RepliconServiceOperator(
            task_id="get_servicecenter_as_department",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 10000,
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

        get_department_as_costcenter = rail.RepliconServiceOperator(
            task_id="get_department_as_costcenter",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 10000,
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

        get_all_employeetypes = rail.RepliconServiceOperator(
            task_id="get_all_employeetypes",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",
            target='artifact'
        )

        end_groups = rail.EmptyOperator(task_id="end_groups")

        start_groups >> get_location_group_as_orgstructure >> get_servicecenter_as_department \
            >> get_department_as_costcenter >> get_all_employeetypes >> end_groups

        return groups_data

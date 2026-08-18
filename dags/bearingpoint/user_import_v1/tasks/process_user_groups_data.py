from bearingpoint.user_import_v1.utils import custom_methods
import rail
null = None


def get_all_groups_data():
    with rail.TaskGroup(
        group_id="get_all_groups_data",
        prefix_group_id=False
    ) as groups_data:

        start_groups = rail.EmptyOperator(task_id="start_groups")

        get_required_costcenter = rail.RepliconServiceOperator(
            task_id="get_required_costcenter",
            endpoint="/services/CostCenterService1.svc/GetEnabledCostCenters",
            data_handler=lambda response, dag_run: custom_methods.get_required_costcenter(response, dag_run)
        )

        get_required_department = rail.RepliconServiceOperator(
            task_id="get_required_department",
            endpoint="/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups",
            data_handler=lambda response, dag_run: custom_methods.get_required_department(response, dag_run)
        )

        get_required_employeetype = rail.RepliconServiceOperator(
            task_id="get_required_employeetype",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",
            data_handler=lambda response, dag_run: custom_methods.get_required_employeetype(response, dag_run)
        )

        get_required_location = rail.RepliconServiceOperator(
            task_id="get_required_location",
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
            data_handler=lambda response, dag_run: custom_methods.get_required_location(response, dag_run)
        )

        get_required_servicecenter = rail.RepliconServiceOperator(
            task_id="get_required_servicecenter",
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
            data_handler=lambda response, dag_run: custom_methods.get_required_servicecenter(response, dag_run)
        )

        end_groups = rail.EmptyOperator(task_id="end_groups")

        start_groups >> get_required_costcenter >> get_required_department >> get_required_employeetype \
            >> get_required_location >> get_required_servicecenter >> end_groups

        return groups_data

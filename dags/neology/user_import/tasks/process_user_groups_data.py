from neology.user_import.utils import custom_methods
import rail


def get_all_groups_data():
    with rail.TaskGroup(
        group_id="get_all_groups_data",
        prefix_group_id=False
    ) as groups_data:

        start_groups = rail.EmptyOperator(task_id="start_groups")

        get_required_department = rail.RepliconServiceOperator(
            task_id="get_required_department",
            endpoint="/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups",
            data_handler=lambda response, dag_run: custom_methods.get_required_department(response, dag_run)
        )

        get_required_location = rail.RepliconServiceOperator(
            task_id="get_required_location",
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
            data_handler=lambda response, dag_run: custom_methods.get_required_location(response, dag_run)
        )

        get_required_division = rail.RepliconServiceOperator(
            task_id="get_required_division",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
            data_handler=lambda response, dag_run: custom_methods.get_required_division(response, dag_run)
        )

        end_groups = rail.EmptyOperator(task_id="end_groups")

        start_groups >> get_required_department >> get_required_location >> get_required_division >> end_groups

        return groups_data

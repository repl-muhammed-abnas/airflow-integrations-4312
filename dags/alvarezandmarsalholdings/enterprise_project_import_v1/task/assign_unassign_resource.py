import rail
from alvarezandmarsalholdings.enterprise_project_import_v1.utils import request_payload, response_filter, python_callable

null = None

def assign_unassign_resource(instance):

    with rail.TaskGroup(group_id='assign_unassign_resource', prefix_group_id=False) as assign_unassign_resource:

        dummy_assign_unassign_resource = rail.EmptyOperator(
            task_id="dummy_assign_unassign_resource"
        )

        get_all_task_details_for_project = rail.PythonOperator(
            task_id="get_all_task_details_for_project",
            python_callable=lambda: request_payload.get_updated_task_details(level=3)
        )

        resource_assigned = rail.RepliconServiceOperator(
            task_id='resource_assigned',
            endpoint='/services/TaskService1.svc/BulkGetResourceAssignments',
            data=request_payload.get_resource_assignment_payload,
            data_handler=lambda response, dag_run: response_filter.get_task_resource(response, dag_run)
        )

        get_resources_add_remove = rail.PythonOperator(
            task_id=f"get_resources_add_remove",
            python_callable=lambda dag_run:python_callable.get_add_remove_resource(dag_run, instance)
        )

        dummy_finish = rail.EmptyOperator(
            task_id="dummy_finish"
        )

        dummy_assign_unassign_resource >> get_all_task_details_for_project >> resource_assigned >> get_resources_add_remove >> dummy_finish

    return dummy_assign_unassign_resource, assign_unassign_resource

import rail
from alvarezandmarsalholdings.customer_project_import_v1.utils import request_payload, response_filter, python_callable

null = None

def assign_unassign_resource():

    with rail.TaskGroup(group_id='assign_unassign_resource', prefix_group_id=False) as assign_unassign_resource:

        dummy_assign_unassign_resource = rail.EmptyOperator(
            task_id="dummy_assign_unassign_resource"
        )

        def get_project_uri():
            return rail.result('update_project')['uri'] if request_payload.does_project_code_exist() else \
                    rail.result('create_project')['uri']

        get_all_task_details_for_project = rail.RepliconServiceOperator(
            task_id="get_all_task_details_for_project",
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data=lambda: {
                "parentUri": get_project_uri()
            },
            data_handler=response_filter.format_project_task_details
        )

        resource_assigned = rail.RepliconServiceOperator(
            task_id='resource_assigned',
            endpoint='/services/TaskService1.svc/BulkGetResourceAssignments',
            data=request_payload.get_resource_assignment_payload,
            data_handler=lambda response, dag_run: response_filter.get_task_resource(response, dag_run)
        )

        get_resources_add_remove = rail.PythonOperator(
            task_id=f"get_resources_add_remove",
            python_callable=python_callable.get_add_remove_resource
        )
        
        add_task_resource = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_task_resource',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            items= '{{ result("get_resources_add_remove").resource_to_add | to_json }}',
            data=lambda item: {
                "taskUri": item['task_uri'],
                "resourceUris": item['uris'],
                "isAssigned": "true"
            }
        )

        remove_task_resource = rail.RepliconServiceCallForEachItemOperator(
            task_id="remove_task_resource",
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            items= '{{ result("get_resources_add_remove").resource_to_remove | to_json }}',
            data=lambda item: {
                "taskUri": item['task_uri'],
                "resourceUris": item['uris'],
                "isAssigned": "false"
            }
        )

        dummy_finish = rail.EmptyOperator(
            task_id="dummy_finish"
        )

        dummy_assign_unassign_resource >> get_all_task_details_for_project >> resource_assigned >> \
        get_resources_add_remove >> rail.Label(
            "Yes") >> add_task_resource >> remove_task_resource >> dummy_finish

    return dummy_assign_unassign_resource, assign_unassign_resource

import rail
from dxctechnology.gsap_task_import.utils import request_payload, custom_methods


def get_create_gsap_task_group(project_type):
    with rail.TaskGroup(group_id="create_gsap_task", prefix_group_id=False) as create_project_task:

        is_date_range_valid = rail.IfOperator(
            task_id="is_date_range_valid",
            test=custom_methods.compare_start_end_date,
            yes_task="create_task",
            no_task="log_date_outside_project_date" if project_type == "gsap" else []
        )

        create_task = rail.RepliconServiceOperator(
            task_id="create_task",
            endpoint="/services/ProjectService1.svc/PutTask",
            data=request_payload.get_add_gsap_task_payload
        )

        has_any_users_to_assign = rail.IfOperator(
            task_id="has_any_users_to_assign",
            test="{{dag_run.conf.user_list | length > 0}}",
            yes_task="add_users_to_task",
            no_task="finish"
        )

        add_users_to_task = rail.RepliconServiceOperator(
            task_id="add_users_to_task",
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda dag_run: {
                "taskUri": rail.result('create_task')['uri'],
                "resourceUris": dag_run.conf['user_list'],
                "isAssigned": "true"
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        is_date_range_valid >> rail.Label("Yes") >> create_task
        create_task >> has_any_users_to_assign

        has_any_users_to_assign >> rail.Label(
            "Yes") >> add_users_to_task >> finish
        has_any_users_to_assign >> rail.Label("No") >> finish

    return create_project_task if project_type != "gsap" else is_date_range_valid, finish

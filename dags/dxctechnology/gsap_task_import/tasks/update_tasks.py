from datetime import timedelta
import rail
from dxctechnology.gsap_task_import.utils import custom_methods, request_payload


def update_tasks(config, project_type, create_task_dag_id):
    with rail.TaskGroup(group_id="update_project_task", prefix_group_id=False) as update_project_task:

        is_date_range_valid = rail.IfOperator(
            task_id="is_date_range_valid",
            test=custom_methods.compare_start_end_date,
            yes_task="does_this_task_already_exist",
            no_task="log_date_outside_project_date" if project_type == "gsap" else []
        )

        does_this_task_already_exist = rail.IfOperator(
            task_id="does_this_task_already_exist",
            test="{{ dag_run.conf.existing_task | is_truthy }}",
            yes_task='can_update_task',
            no_task='create_task',
        )

        can_update_task = rail.IfOperator(
            task_id="can_update_task",
            test=custom_methods.can_update_task,
            yes_task="update_task",
            no_task="log_unchanged_record" if project_type == "gsap" else []
        )

        update_task = rail.RepliconServiceOperator(
            task_id="update_task",
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_update_gsap_task_payload
        )

        create_task = rail.TriggerDagRunForEachItemOperator(
            task_id="create_task",
            items=[1],
            trigger_dag_id=create_task_dag_id,
            conf=request_payload.get_create_gsap_task_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_create_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_task',
            dag_runs='{{ result("create_task") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        is_date_range_valid >> rail.Label("Yes") >> does_this_task_already_exist >> rail.Label(
            "Yes") >> can_update_task
        can_update_task >> rail.Label("Yes") >> update_task
        does_this_task_already_exist >> rail.Label(
            "No") >> create_task >> wait_for_create_task

        return update_project_task if project_type != "gsap" \
            else is_date_range_valid, can_update_task, update_task, wait_for_create_task

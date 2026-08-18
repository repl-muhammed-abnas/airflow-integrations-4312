from datetime import timedelta
import rail
from dxctechnology.c1_task_import import request_payload
from dxctechnology.c1_task_import import custom_method


def update_tasks(config, project_type):
    with rail.TaskGroup(group_id="update_project_task", prefix_group_id=False) as update_project_task:

        is_date_range_valid = rail.IfOperator(
            task_id="is_date_range_valid",
            test=custom_method.compare_start_end_date,
            yes_task="does_this_task_already_exist",
            no_task="log_date_outside_project_date" if project_type == "c1" else []
        )

        does_this_task_already_exist = rail.IfOperator(
            task_id="does_this_task_already_exist",
            test="{{ dag_run.conf.existing_tasks | is_truthy }}",
            yes_task='can_update_task',
            no_task='create_task',
        )

        can_update_task = rail.IfOperator(
            task_id="can_update_task",
            test=custom_method.can_update_task,
            yes_task="update_task",
            no_task="log_unchanged_record" if project_type == "c1" else []
        )

        update_task = rail.RepliconServiceOperator(
            task_id="update_task",
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_update_c1_task_payload
        )

        create_task = rail.TriggerDagRunForEachItemOperator(
            task_id="create_task",
            items=[1],
            trigger_dag_id=f"dxctechnology_c1_task_import_child_create_{project_type}_task_{config.instance}",
            conf=request_payload.get_create_c1_task_conf,
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

        return update_project_task if project_type != "c1" \
            else is_date_range_valid, can_update_task, update_task, wait_for_create_task

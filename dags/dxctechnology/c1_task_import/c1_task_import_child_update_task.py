from datetime import timedelta
import rail
from dxctechnology.c1_task_import import request_payload
# from dxctechnology.c1_task_import.tasks.update_task import update_tasks
from dxctechnology.c1_task_import import custom_method

# config: https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_task_import/config.py


# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_c1_task_import_child_update_c1_task_{config.instance}",
        description=f"DXCTechnology C1 Task Import Update C1 Task {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.child_dag_update_task_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        project_type = 'c1'
        can_update_task = rail.IfOperator(
            task_id="can_update_task",
            test=custom_method.can_update_task,
            yes_task="update_task",
            no_task="log_unchanged_record" if project_type == "c1" else []
        )

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

        log_date_outside_project_date = rail.WriteLogOperator(
            task_id='log_date_outside_project_date',
            message="{{dag_run.conf.task_name}}'s given date is outside of project start, end date",
            items='[{{dag_run.conf | to_json}}]',
            severity="skipped",
            properties=custom_method.get_log_out_of_range,
        )

        log_unchanged_record = rail.WriteLogOperator(
            task_id='log_unchanged_record',
            message='{{dag_run.conf.task_name}} No change to task record',
            severity="skipped",
            properties={
                    'wbs': '{{ dag_run.conf.project_name }}',
                    'task': '{{ dag_run.conf.task_name }}',
                    'status': 'skipped',
                    'details': "No change to task record"
            },
        )

        log_successful_update_completion = rail.WriteLogOperator(
            task_id='log_successful_update_completion',
            message='{{dag_run.conf.task_name}} Updated successfully',
            severity="Success",
            properties={
                    'wbs': '{{ dag_run.conf.project_name}}',
                    'task': '{{ dag_run.conf.task_name }}',
                    'status': 'Success',
                    'details': "Updated successfully"
            },
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                    'wbs': '{{ dag_run.conf.project_name }}',
                    'task': '{{ dag_run.conf.task_name }}',
                    'status': "Error",
                    'details': '{{ get_error_message() }}'
            },
        )

        update_task >> log_successful_update_completion >> rail.Label(
            "On error") >> catch_and_log_errors
        is_date_range_valid >> rail.Label("Yes") >> does_this_task_already_exist >> rail.Label(
            "Yes") >> can_update_task
        does_this_task_already_exist >> rail.Label(
            "Yes") >> can_update_task
        can_update_task >> rail.Label("Yes") >> update_task
        can_update_task >> rail.Label("No") >> log_unchanged_record >> rail.Label(
            "On error") >> catch_and_log_errors
        does_this_task_already_exist >> rail.Label(
            "No") >> create_task >> wait_for_create_task
        wait_for_create_task >> rail.Label("On error") >> catch_and_log_errors
        is_date_range_valid >> rail.Label("No") >> log_date_outside_project_date >> rail.Label(
            "On error") >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)

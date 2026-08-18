import rail
from dxctechnology.c1_task_import.tasks.create_task import create_tasks
from dxctechnology.c1_task_import import custom_method

# config: https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_task_import/config.py


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_c1_task_import_child_create_c1_task_{config.instance}",
        description=f"DXCTechnology C1 Task Import create C1 Task {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.child_dag_create_task_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_date_range_valid = rail.IfOperator(
            task_id="is_date_range_valid",
            test=custom_method.compare_start_end_date,
            yes_task="create_task",
            no_task="log_date_outside_project_date"
        )

        create_task, finish = create_tasks(
            "c1")

        log_date_outside_project_date = rail.WriteLogOperator(
            task_id='log_date_outside_project_date',
            message="{{dag_run.conf.task_name}}'s given date is outside of project start, end date",
            items='[{{dag_run.conf | to_json}}]',
            severity="skipped",
            properties=custom_method.get_log_out_of_range,
        )

        log_successful_task_create_completion = rail.WriteLogOperator(
            task_id='log_successful_task_create_completion',
            message='{{dag_run.conf.task_name}} Created successfully',
            severity="Success",
            properties={
                    'wbs': '{{ dag_run.conf.project_name}}',
                    'task': '{{ dag_run.conf.task_name }}',
                    'status': 'Success',
                    'details': "Created successfully"
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
            }
        )

        is_date_range_valid >> rail.Label("Yes") >> create_task
        is_date_range_valid >> rail.Label(
            "No") >> log_date_outside_project_date
        finish >> log_successful_task_create_completion
        log_successful_task_create_completion >> rail.Label(
            "On error") >> catch_and_log_errors
        log_date_outside_project_date >> rail.Label(
            "On error") >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)

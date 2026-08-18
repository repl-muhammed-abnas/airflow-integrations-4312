import rail
from itvdaytime.user_import.tasks.supervisor_task import get_supervisor_task


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"itvdaytime_user_import_process_supervisor_assignment_{config.instance}",
        description=f"iTV DayTime User Import process process_supervisor_assignment {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        _, supervisor_end = get_supervisor_task(
            user_uri="{{dag_run.conf.user_uri}}",
            is_update_user=True,
            caller="process_supervisor"
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.loginname}}",
                "status": "Error",
                "action": "{{dag_run.conf.action}}",
                "details": "User added successfully",
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "{{dag_run.conf.user_uri}}",
                "allowed_for_supervisor_processing": "No"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        supervisor_end >> catch_and_log_errors >> log_to_sumo
        return dag


rail.for_each_instance(create_child_dag)

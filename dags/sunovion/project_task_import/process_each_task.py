from datetime import timedelta
from airflow.models import Variable
from sunovion.project_task_import.utils import request_payload
import rail


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'sunovion_project_sync_process_each_task_child_{config.instance}',
        description='Sunovion Project and Task Sync - Process Each Task',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_code,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='false').lower() == 'true',
            yes_task="batch_task",
            no_task="get_task_details"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_task_details',
            end_task="catch_and_log_errors",
        )

        get_task_details = rail.PythonOperator(
            task_id="get_task_details",
            python_callable=request_payload.get_task_details
        )

        is_project_task_present = rail.IfOperator(
            task_id='is_project_task_present',
            test=request_payload.is_project_task_present,
            yes_task='update_project_task',
            no_task='is_project_task_not_present'
        )

        update_project_task = rail.TriggerDagRunForEachItemOperator(
            task_id='update_project_task',
            items=['one_run'],
            trigger_dag_id=f'sunovion_project_sync_process_update_task_child_{config.instance}',
            conf=request_payload.process_update_task_conf,
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            retries=0,
        )

        wait_for_update_project_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_project_task',
            dag_runs='{{ result("update_project_task") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        is_project_task_not_present = rail.IfOperator(
            task_id='is_project_task_not_present',
            test=request_payload.is_project_task_not_present,
            yes_task='create_project_task'
        )

        create_project_task = rail.TriggerDagRunForEachItemOperator(
            task_id='create_project_task',
            items=['one_run'],
            trigger_dag_id=f'sunovion_project_sync_process_create_task_child_{config.instance}',
            conf=request_payload.process_create_task_conf,
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            retries=0,
        )

        wait_for_create_project_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_project_task',
            dag_runs='{{ result("create_project_task") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log }}',
            severity='Failed',
            message='{{ get_error_message() }}',
            properties={
                    'projectcode': '{{ dag_run.conf.items.projectcode }}',
                    'taskcode': '{{ dag_run.conf.taskcode }} / {{ dag_run.conf.taskname }}',
                    'status': 'Failed',
                    'details': '{{ get_error_message() }}',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_task_details
        get_task_details >> is_project_task_present >> rail.Label(
            "Yes") >> update_project_task >> wait_for_update_project_task >> catch_and_log_errors
        is_project_task_present >> rail.Label(
            "No") >> is_project_task_not_present
        is_project_task_not_present >> rail.Label(
            "Yes") >> create_project_task >> wait_for_create_project_task >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)

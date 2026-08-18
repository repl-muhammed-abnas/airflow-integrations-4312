from datetime import timedelta
from airflow.models import Variable
from sunovion.project_task_import.utils import request_payload
from sunovion.project_task_import.utils import response_filter
import rail


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'sunovion_project_sync_process_each_code_child_{config.instance}',
        description='Sunovion Project and Task Sync - Process Each Code',
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
            no_task="get_project_details"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_project_details',
            end_task="catch_and_log_errors",
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="services/ProjectListService1.svc/GetData",
            data=request_payload.get_project_details,
            response_filter=response_filter.get_specific_project_uri
        )

        is_project_present = rail.IfOperator(
            task_id='is_project_present',
            test=lambda: bool(rail.result('get_project_details')),
            yes_task='update_project',
            no_task='create_project'
        )

        update_project = rail.TriggerDagRunForEachItemOperator(
            task_id='update_project',
            items=["one_run"],
            trigger_dag_id=f'sunovion_project_sync_update_project_child_{config.instance}',
            conf=request_payload.get_update_project_conf,
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            retries=0,
        )

        wait_for_update_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_project',
            dag_runs='{{ result("update_project") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        create_project = rail.TriggerDagRunForEachItemOperator(
            task_id='create_project',
            items=["one_run"],
            trigger_dag_id=f'sunovion_project_sync_create_project_child_{config.instance}',
            conf=request_payload.get_create_project_conf,
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            retries=0,
        )

        wait_for_create_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_project',
            dag_runs='{{ result("create_project") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log }}',
            severity='Failed',
            message='{{ get_error_message() }}',
            properties={
                    'projectcode': '{{ dag_run.conf.item.projectcode }} / {{ dag_run.conf.item.projectname }}',
                    'taskcode': '-',
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
        can_run_batch_task >> rail.Label("No") >> get_project_details
        get_project_details >> is_project_present >> rail.Label(
            "Yes") >> update_project >> wait_for_update_project >> catch_and_log_errors
        is_project_present >> rail.Label(
            "No") >> create_project >> wait_for_create_project >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)

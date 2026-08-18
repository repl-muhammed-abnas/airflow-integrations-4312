from datetime import timedelta
from pendulum import datetime as dt
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/cbreinc/add_projects_to_client/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'cbreinc_add_project_to_client_master_{config.instance}',
        description=f'CBREInc Add Projects To Client Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        start_date=dt(2023, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval
    ) as dag:

        start = rail.EmptyOperator(
            task_id="start"
        )

        get_webhook_log = rail.CreateLogOperator(
            task_id="get_webhook_log",
            tenant_wide_name="cbreinc_webhook_add_project_to_client",
            existing_log_mode="truncate",
        )

        has_any_data = rail.HasDataOperator(
            task_id="has_any_data",
            source="{{ result('get_webhook_log', 'truncated_data') }}",
            yes_task='write_csv_backup',
            no_task='finish'
        )

        write_csv_backup = rail.WriteCSVFileOperator(
            task_id="write_csv_backup",
            source="{{ result('get_webhook_log', 'truncated_data') }}",
            header=[
                'execution-correlation-id',
                'project-uri',
                'project-name',
                'event-type'],
            row=['{{ item.ecid }}', '{{ item.properties.project_uri }}', '{{ item.properties.project_name }}', '{{ item.message }}'],
        )

        create_events_collection = rail.CreateCollectionOperator(
            task_id='create_events_collection',
            source="{{ result('write_csv_backup') }}",
        )

        query_projects = rail.QueryCollectionOperator(
            task_id='query_projects',
            query='SELECT * FROM create_events_collection',
        )

        process_each_project = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_project',
            retries=0,
            items=lambda: rail.load_all_records(rail.result("query_projects")),
            trigger_dag_id=f'cbreinc_add_client_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'project_uri': item["project_uri"],
                'project_name': item["project_name"]
            }
        )

        wait_for_process_each_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_project',
            dag_runs='{{ result("process_each_project") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        start >> get_webhook_log >> has_any_data >> write_csv_backup >> create_events_collection >> query_projects\
        >> process_each_project >> wait_for_process_each_project >> finish
        return dag

rail.for_each_instance(create_dag)

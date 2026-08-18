from datetime import timedelta
from pendulum import datetime
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"Wipro Auto Shift Assignment Monthly {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_country',
            retries=0,
            items=[1],
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "country": dag_run.conf.get('country'),
                "month": dag_run.conf.get('month'),
                "default_shift": dag_run.conf.get('default_shift'),
                "start_date": dag_run.conf.get("start_date")
            }
        )

    return dag


rail.for_each_instance(create_main_dag)

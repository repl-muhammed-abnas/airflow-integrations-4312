from datetime import timedelta
from pendulum import datetime
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"Wipro Auto Shift Assignment removal for holidays {config.instance}",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2025, 2, 1, tz=config.time_zone),
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_country',
            retries=0,
            items=config.COUNTRY_MONTH_SHIFT_ASSIGNMENT,
            trigger_dag_id=config.process_each_country_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "country": item['country'],
                "default_shift": item['default_shift']
            }
        )

    return dag


rail.for_each_instance(create_main_dag)

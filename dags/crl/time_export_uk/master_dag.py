from pendulum import datetime, now
import rail
from crl.time_export_uk.utils.custom_methods import EXPORT_DATE_FORMAT, get_time_export_name


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_export_master_dag_id,
        description="CRL UK Time Export Master",
        start_date=datetime(2025, 1, 1, tz=config.time_zone),
        company_key=config.company_key,
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        rail.TriggerDagRunOperator(
            task_id="process_time_export",
            trigger_dag_id=config.time_export_process_export_dag_id,
            conf=lambda dag_run: {
                **{
                    "todays_date": now(tz=config.time_zone).strftime(EXPORT_DATE_FORMAT),
                    "timezone": config.time_zone,
                    "process_start_time": now(tz=config.time_zone).strftime('%Y-%m-%dT%H:%M:%S')
                },
                **get_time_export_name(config),
                "start_date": dag_run.conf.get('start_date') if dag_run.conf else None,
                "end_date": dag_run.conf.get('end_date') if dag_run.conf else None
            }
        )

    return dag


rail.for_each_instance(create_main_dag)
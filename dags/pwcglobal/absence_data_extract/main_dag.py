from pendulum import datetime
import rail
from pwcglobal.absence_data_extract.task.location_export import location_export_task

# config: https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/absence_data_extract/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_absence_data_extract_master_{config.instance}_{config.location_code}",
        description=f"PWC Global Absence data extract Master {config.instance} {config.location}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1, tz=config.schedule_timezone),
        schedule_interval=config.schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    )as dag:

        location_export_task(config)

    return dag


rail.for_each_instance(create_dag)

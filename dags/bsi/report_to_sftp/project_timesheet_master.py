import rail
from pendulum import datetime
from bsi.report_to_sftp.tasks.report_export_to_sftp import process_report_to_sftp


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'bsi_project_timesheet_daily_master_{config.instance}',
        description='BSI Extract Report Project Timesheet Daily',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_daily,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        process_report_to_sftp(config,config.extract_project_timesheet_details_daily,config.extract_project_timesheet_daily_file_name,)

    return dag


rail.for_each_instance(create_main_airflow_dag)

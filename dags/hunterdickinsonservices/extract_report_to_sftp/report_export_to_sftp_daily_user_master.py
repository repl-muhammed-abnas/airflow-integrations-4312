import rail
from pendulum import datetime
from hunterdickinsonservices.extract_report_to_sftp.tasks.report_export_to_sftp_child import process_report_to_sftp


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'hunterdickinson_report_export_to_sftp_user_daily_master_{config.instance}',
        description='Hunter Dickinson Extract Report User Daily',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_daily,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        process_report_to_sftp(config, config.extract_report_name_user_daily,
                     config.extract_report_user_daily_file_name, config.extract_report_daily_file_path)

    return dag


rail.for_each_instance(create_main_airflow_dag)

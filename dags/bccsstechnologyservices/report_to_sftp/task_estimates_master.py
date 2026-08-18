import rail
from pendulum import datetime
from bccsstechnologyservices.report_to_sftp.tasks.report_export_to_sftp_replace_existing_file import process_report_to_sftp


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'bccss_report_to_sftp_task_estimates_master_{config.instance}',
        description='BCCSSTechnologyServices Report To Sftp',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.new_schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        process_report_to_sftp(config, config.extract_report_name_task_estimates,
                         config.extract_report_task_estimates_file_name)

    return dag


rail.for_each_instance(create_main_airflow_dag)

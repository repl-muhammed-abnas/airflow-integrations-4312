from datetime import datetime, timedelta
import rail


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_testing_dag_for_sftp_file_download_{config.region.replace('-', '_')}_{config.instance}",
        description=f'Testing File Download from SFTP {config.region} {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(minutes=2),
        start_date=datetime(2022, 1, 1),
        max_active_runs = config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:


        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        new_file_sensor >> download_file

    return dag


rail.for_each_instance(create_airflow_dag)

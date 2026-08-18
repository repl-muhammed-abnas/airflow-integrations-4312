from pendulum import datetime

import rail
from strayeruniversity.fmla_extract.utils import python_callable


# config : https://github.com/replicon/airflow-integrations/blob/main/dags/strayeruniversity/fmla_extract/config.py

# pylint: disable=too-many-statements


def create_child_task_create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'strayeruniversity_move_file_archive_child_{config.instance}',
        description=f'Strayer University Move File to Archive {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        start_date=datetime(2022, 10, 10, tz=config.time_zone),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_last_modified_time = rail.PythonOperator(
            task_id='get_last_modified_time',
            python_callable=lambda dag_run: python_callable.get_file_last_modified_time(dag_run, config.time_zone)
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename=config.export_file_path +
            '/{{ dag_run.conf.item.sftp_file_name }}',
            new_filename=config.archive_file_path +
            "/" + config.filename + "_{{ result('get_last_modified_time') }}.csv"
        )

        get_last_modified_time >> archive_file

    return dag


rail.for_each_instance(create_child_task_create_dag)

from pendulum import datetime

import rail


# config : https://github.com/replicon/airflow-integrations/blob/main/dags/genoadesign/report_to_sftp/config.py

# pylint: disable=too-many-statements


def create_child_task_create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'genoa_design_move_file_archive_child_{config.instance}',
        description=f'Genoa Design Move File to Archive {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_runs,
        start_date=datetime(2022, 10, 10, tz=config.time_zone),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{dag_run.conf.export_path}}' +
            '/{{ dag_run.conf.item.sftp_file_name }}',
            new_filename=config.archive_file_path +
            "/{{ dag_run.conf.item.sftp_file_name }}"
        )

    return dag


rail.for_each_instance(create_child_task_create_dag)

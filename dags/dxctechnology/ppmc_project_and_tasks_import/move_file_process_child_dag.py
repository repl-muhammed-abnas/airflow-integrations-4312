from datetime import datetime

import rail
from dxctechnology.ppmc_project_and_tasks_import import request_payload

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/ppmc_project_and_tasks_import/config.py

# pylint: disable=too-many-statements


def create_child_task_create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_ppmc_project_task_import_move_file_process_child_{config.instance}',
        description=f'PPMC - Move file to processing - Move SFTP File New logic to ignore duplicate {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        start_date=datetime(2022, 1, 1),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_first_index_file = rail.IfOperator(
            task_id='is_first_index_file',
            test=lambda: request_payload.get_dag_run_conf()[
                'item']['index'] == 0,
            yes_task='download_file',
            no_task='move_file'
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath=config.move_file_input_filepath +
            '/' + "{{ dag_run.conf.item.sftp_file_name }}",
        )

        upload_file = rail.SFTPUploadFileOperator(
            task_id='upload_file',
            content="{{ result('download_file') }}",
            remote_filepath=config.move_file_process_filepath +
            '/' + "{{ dag_run.conf.item.sftp_file_name }}"
        )

        move_file = rail.SFTPMoveFileOperator(
            task_id='move_file',
            existing_filename=config.move_file_input_filepath +
            '/{{ dag_run.conf.item.sftp_file_name }}',
            new_filename=config.move_file_archive_filepath +
            "/{{ '' if dag_run.conf.item.index == 0 else 'Ignored_'}}{{ dag_run_ecid() | replace(':', '-') }}_{{ dag_run.conf.item.sftp_file_name }}"
        )

        is_first_index_file >> rail.Label(
            'yes') >> download_file >> upload_file >> move_file
        is_first_index_file >> rail.Label('No') >> move_file

    return dag


rail.for_each_instance(create_child_task_create_dag)

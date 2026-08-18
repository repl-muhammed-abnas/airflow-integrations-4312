from datetime import datetime

import rail


# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/ppmc_project_and_tasks_import/config.py

# pylint: disable=too-many-statements


def create_child_task_create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_ppmc_project_task_import_move_file_archive_child_{config.instance}',
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

        rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename=config.move_file_input_filepath +
            '/{{ dag_run.conf.item.sftp_file_name }}',
            new_filename=config.move_file_archive_filepath +
            "/Ignored_{{ dag_run_ecid() | replace(':', '-') }}_{{ dag_run.conf.item.sftp_file_name }}"
        )

    return dag


rail.for_each_instance(create_child_task_create_dag)

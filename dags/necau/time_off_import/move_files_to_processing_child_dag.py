import rail
# pylint: disable=too-many-statements


def create_child_task_create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'necau_move_files_to_processing_child_{config.instance}',
        description=f'NECAU - Move file to processing {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        move_files_to_processing = rail.SFTPMoveFileOperator(
            task_id='move_files_to_processing',
            existing_filename=config.timeoff_import_file_directory +
            '/{{ dag_run.conf.file_name }}',
            new_filename=config.processing_file_directory +
            '/{{ dag_run.conf.file_name }}'
        )

        move_files_to_processing

    return dag


rail.for_each_instance(create_child_task_create_dag)

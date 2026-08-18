from pendulum import datetime
import rail

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.move_to_processing_master_dag_id,
        description=f'DXC GSAP IWO Resource Assignment Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_move_to_processing,
        start_date=datetime(2022, 4, 1, tz=config.utc_timezone),
        schedule_interval=config.schedule,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        list_sftp_files = rail.SFTPListFilesOperator(
            task_id='list_sftp_files',
            paths=[config.move_file_input_filepath]
        )

        has_files = rail.IfOperator(
            task_id='has_files',
            test=lambda: bool(rail.result('list_sftp_files').get(
                config.move_file_input_filepath)),
            yes_task='for_each_sftp_files',
            no_task='finish'
        )

        for_each_sftp_files = rail.ForEachOperator(
            task_id='for_each_sftp_files',
            items=lambda: rail.result('list_sftp_files')[config.move_file_input_filepath],
            start_task='upload_sftp_files_to_process',
            end_task='for_each_sftp_files_end'
        )

        upload_sftp_files_to_process = rail.SFTPMoveFileOperator(
            task_id='upload_sftp_files_to_process',
            existing_filename=config.move_file_input_filepath +
            '/{{ result("for_each_sftp_files").name }}',
            new_filename=config.input_filepath +
            '/{{ result("for_each_sftp_files").name }}'
        )

        for_each_sftp_files_end = rail.EmptyOperator(
            task_id='for_each_sftp_files_end'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        list_sftp_files >> has_files >> rail.Label('Yes') >> for_each_sftp_files >> upload_sftp_files_to_process >> for_each_sftp_files_end
        has_files >> rail.Label('No') >> finish
        for_each_sftp_files >> for_each_sftp_files_end
        for_each_sftp_files_end >> finish


    return dag


rail.for_each_instance(create_dag)

import rail
from necau.time_off_import.utils import python_callable_method
# pylint: disable=too-many-statements


def create_child_task_create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'necau_archiving_timeoff_file_child_{config.instance}',
        description=f'NECAU - Move file to unprocessed {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename=config.timeoff_import_file_directory +
            '/{{ dag_run.conf.file_name }}',
            new_filename=config.unprocessed_file_directory +
            '/{{ dag_run.conf.file_name }}'
        )

        download_warning_file = rail.SFTPDownloadFileOperator(
            task_id='download_warning_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.unprocessed_file_directory +
            '/{{ dag_run.conf.file_name }}'
        )

        parse_warning_file = rail.LoadCSVFileOperator(
            task_id="parse_warning_file",
            document="{{result('download_warning_file')}}",
            headers=["Staff Member", "Surname", "Preferred Name",
                     "Form Code", "Form Description", "Request Key", "Creation Date",
                     "Creation Time", "Seq No", "Leave Type", "Leave Description",
                     "Start Date", "End Date", "Action Status", "Days Taken", "Hours Taken"]
        )

        warning_file_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='warning_file_download_link',
            artifact_name="{{ result('parse_warning_file')}}",
            output_file_name='{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        add_files_with_names = rail.PythonOperator(
            task_id='add_files_with_names',
            python_callable=python_callable_method.get_file_info
        )

        archive_file >> download_warning_file >> parse_warning_file >> warning_file_download_link >> add_files_with_names

    return dag


rail.for_each_instance(create_child_task_create_dag)

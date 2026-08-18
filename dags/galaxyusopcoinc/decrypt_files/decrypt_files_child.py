from pendulum import datetime
import rail

def create_disable_user_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"vialtopartners_decrypt_input_files_child{config.instance}",
        description=f'vialtopartners_decrypt_input_files_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1),
        max_active_runs=config.decrypt_file_child_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        download_file = rail.SFTPDownloadFileOperator(
            task_id = "download_file",
            remote_filepath="{{dag_run.conf.file_full_path}}"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id = "archive_file",
            existing_filename="{{dag_run.conf.file_full_path}}",
            new_filename= config.archive_input_filepath+"/{{dag_run.conf.file_name}}"
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        upload_file = rail.SFTPUploadFileOperator(
            task_id = "upload_file",
            content= "{{result('decrypt_file')}}",
            remote_filepath=config.decrypted_file_upload_path + "/{{ dag_run.conf.file_name}}"
        )

        download_file >> archive_file >> decrypt_file >> upload_file

    return dag

rail.for_each_instance(create_disable_user_main_dag)

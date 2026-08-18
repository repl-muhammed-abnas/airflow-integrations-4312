
from datetime import timedelta
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'zaloragroup_pgp_decryptingfiles_timeoff_file_{config.instance}',
        description=f'PGP_ Decrypting files_Timeoff File {config.instance}',
        company_key=config.company_key,
        schedule_interval=timedelta(seconds=config.decrypt_files_dag_interval),
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id = 'download_file',
            sftp_conn_id= config.sftp_conn_id,
            remote_filepath= "{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='move_file_to_archive',
            no_task='delete_this_dagrun',
        )

        move_file_to_archive = rail.SFTPMoveFileOperator(
            task_id='move_file_to_archive',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath + "Encrypted_"+"{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            retries=0,
            source="{{ result('download_file') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_sftp',
            sftp_conn_id= config.sftp_conn_id,
            content="{{ result('decrypt_file') }}",
            remote_filepath=config.upload_filepath +
            "{{result('new_file_sensor') | file_name | replace('.gpg','') }}"
        )


        new_file_sensor >>  download_file >> was_new_file_found >> rail.Label(
            "Yes") >> move_file_to_archive
        was_new_file_found >> rail.Label(
            "No") >> delete_this_dagrun
        move_file_to_archive >> decrypt_file >> upload_file_to_sftp

    return dag

rail.for_each_instance(create_dag)

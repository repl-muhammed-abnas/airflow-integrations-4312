
from datetime import timedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'zaloragroup_user_import_decrypting_files_{config.instance}_v1',
        description=f'zaloragroup_user_import_decrypting_files_{config.instance}_v1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.decryption_dag_active_runs,
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
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source="{{ result('download_file') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_sftp',
            content="{{ result('decrypt_file') }}",
            remote_filepath=config.input_filepath_master +  "/{{result('new_file_sensor') | file_name | replace('.gpg','') }}"
        )

        move_file_to_archive = rail.SFTPMoveFileOperator(
            task_id='move_file_to_archive',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath + "/Encrypted_" + "{{ result('new_file_sensor') | file_name }}"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor >> download_file >> decrypt_file >> upload_file_to_sftp >> \
            move_file_to_archive >> log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)

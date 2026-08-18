from datetime import timedelta, datetime
import rail
from airflow.models import Variable

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_workday_user_sync_decrypt_file_{config.instance}',
        description=f'dxctechnology_workday_user_sync_decrypt_file{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 9, 26),
        schedule_interval=timedelta(seconds=config.decryption_schedule_interval),
        max_active_runs=config.decryption_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="new_file_sensor"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="new_file_sensor",
            end_task="upload_decrypted_file_to_sftp",
            execution_timeout=timedelta(days=14)
        )

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_file_path,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule="all_done",
            test='{{get_task_state("new_file_sensor") == "success" }}',
            yes_task="download_file",
            no_task="delete_dagrun"
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
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

        upload_decrypted_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_decrypted_file_to_sftp',
            content="{{ result('decrypt_file') }}",
            remote_filepath=config.archive_file_path +  "/Decrypted_{{result('new_file_sensor') | file_name | replace('.csv.pgp','') | replace('.csv','') }}" + ".csv"
        )


        can_run_batch_task >> rail.Label("Yes") >> batch_task >> upload_decrypted_file_to_sftp
        can_run_batch_task >> rail.Label("No") >> new_file_sensor

        new_file_sensor >> was_new_file_found
        was_new_file_found >> rail.Label('No') >> delete_dagrun
        was_new_file_found >> rail.Label('Yes') >> download_file >> decrypt_file >> upload_decrypted_file_to_sftp
    return dag

rail.for_each_instance(create_dag)

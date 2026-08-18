from datetime import timedelta
import rail
from cargolux.sftp_transfer.utils.custom_functions import cleanup_file

def create_child_dag(config):
    """Create child DAG to process a single file transfer"""

    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description="Process single file transfer from source to destination SFTP",
        company_key=config.company_key,
        schedule_interval=None,  # Child DAGs are triggered, never scheduled
        replicon_conn_id=None,
        integration_type='generic',
        max_active_runs=config.child_max_active_runs,
    ) as dag:

        # View the configuration passed from master
        view_conf = rail.ViewDagRunConfOperator(
            task_id='view_dagrun_conf'
        )

        extract_record = rail.PythonOperator(
            task_id='extract_record',
            python_callable=lambda dag_run: dag_run.conf
        )

        # Download file from source SFTP
        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath=f"{config.source_sftp_input_path}/{{{{ result('extract_record')['file_name'][0]['filename'] }}}}",
            sftp_conn_id=config.source_sftp_conn_id
        )

        # Upload file to destination SFTP
        upload_file = rail.SFTPUploadFileOperator(
            task_id='upload_file',
            content='{{ result("download_file") }}',
            remote_filepath=f"{config.dest_sftp_output_path}{{{{ result('extract_record')['file_name'][0]['filename'] }}}}",
            sftp_conn_id=config.dest_sftp_conn_id
        )

        # Delete file from source after successful upload
        delete_source_file = rail.SFTPDeleteFileOperator(
            task_id='delete_source_file',
            existing_filename=f"{config.source_sftp_input_path}/{{{{ result('extract_record')['file_name'][0]['filename'] }}}}",
            sftp_conn_id=config.source_sftp_conn_id
        )

        # Log successful transfer
        log_success = rail.WriteLogOperator(
            task_id='log_success',
            message="Successfully transferred file"
        )

        # Cleanup local file (always run)
        cleanup_local_file = rail.PythonOperator(
            task_id='cleanup_local_file',
            trigger_rule='all_done',
            python_callable=lambda **context: cleanup_file(context)
        )

        # Handle errors
        log_failure = rail.WriteLogOperator(
            task_id='log_failure',
            trigger_rule='one_failed',
            message="Failed to transfer file"
        )

        # Final task
        finalize = rail.EmptyOperator(
            task_id='finalize',
            trigger_rule='all_done'
        )

        view_conf >> extract_record >> download_file >> upload_file >> delete_source_file >> log_success

        # Cleanup path
        download_file >> cleanup_local_file
        upload_file >> cleanup_local_file
        delete_source_file >> cleanup_local_file
        log_success >> cleanup_local_file

        # Error path
        download_file >> log_failure
        upload_file >> log_failure
        delete_source_file >> log_failure

        # Converge to finalize
        cleanup_local_file >> finalize
        log_failure >> finalize

    return dag

# Create DAG for each instance
rail.for_each_instance(create_child_dag)

from datetime import timedelta
import rail
from cargolux.sftp_transfer.utils.custom_functions import create_log_file

def create_main_dag(config):
    """Create Master DAG for SFTP file transfer orchestration"""

    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=config.dag_description,
        company_key=config.company_key,
        schedule_interval=config.schedule_interval,
        replicon_conn_id=None,
        integration_type='generic',
        max_active_runs=config.max_active_runs,
        
    ) as dag:

        check_for_files = rail.SFTPAnyFileSensor(
            task_id='check_for_files',
            path=f"{config.source_sftp_input_path}/{config.file_pattern}",
            sftp_conn_id=config.source_sftp_conn_id,
            poke_interval=config.poke_interval_seconds,
            soft_fail_timeout=timedelta(minutes=config.sensor_timeout_minutes)
        )

        files_found = rail.IfOperator(
            task_id='files_found',
            test='{{ get_task_state("check_for_files") == "success" }}',
            yes_task='list_files',
            no_task='skip_no_files'
        )

        skip_no_files = rail.EmptyOperator(
            task_id='skip_no_files'
        )

        list_files = rail.SFTPListFilesOperator(
            task_id='list_files',
            paths=[config.source_sftp_input_path],
            sftp_conn_id=config.source_sftp_conn_id
        )

        def extract_file_names():
            """Extract filenames from SFTPListFilesOperator output and format for child DAG"""
            list_result = rail.result('list_files')
            file_items = []
            for path, files in list_result.items():
                for file_info in files:
                    file_items.append({
                        "filename": file_info['name']
                    })
            return file_items

        extract_filenames = rail.PythonOperator(
            task_id='extract_filenames',
            python_callable=extract_file_names
        )

        trigger_file_transfers = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_file_transfers',
            items=lambda: rail.result("extract_filenames"),
            trigger_dag_id=config.child_dag_id,
            batch_size=config.batch_size,
            conf=lambda item: {
                "file_name": item,
            }
        )

        wait_for_transfers = rail.WaitForDagRunsSensor(
            task_id='wait_for_transfers',
            dag_runs='{{ result("trigger_file_transfers") }}',
            execution_timeout=timedelta(hours=2)
        )

        create_transfer_log = rail.PythonOperator(
            task_id='create_transfer_log',
            trigger_rule='all_done',
            python_callable=create_log_file,
            op_kwargs={
                'config': config,
                'execution_date': '{{ ds }}'
            }
        )

        upload_log_file = rail.SFTPUploadFileOperator(
            task_id='upload_log_file',
            content='{{ result("create_transfer_log") }}',
            remote_filepath=f'{config.source_sftp_log_path}{{{{ ds_nodash }}}}_{{{{ ts_nodash }}}}_transfer_log.txt',
            sftp_conn_id=config.source_sftp_conn_id
        )

        # Main flow
        check_for_files >> files_found

        # Conditional branching
        files_found >> rail.Label('Yes') >> list_files
        files_found >> rail.Label('No') >> skip_no_files

        # Process files through child DAGs
        list_files >> extract_filenames >> trigger_file_transfers >> wait_for_transfers

        # Generate summary log from both paths
        wait_for_transfers >> create_transfer_log
        skip_no_files >> create_transfer_log

        # Upload summary log
        create_transfer_log >> upload_log_file

    return dag

# Create DAG for each instance
rail.for_each_instance(create_main_dag)

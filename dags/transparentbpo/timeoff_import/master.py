import rail
from pendulum import datetime, now
from datetime import timedelta
from airflow.models import Variable
from transparentbpo.timeoff_import.utils import custom_methods, request_payload


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='Time off Sync from Bamboo Hr to Replicon Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2026, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        # Full reference file path
        reference_file_fullpath = config.reference_filepath + config.reference_filename

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_job_start_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='log_job_start_time',
            end_task='batch_end',
        )

        log_job_start_time = rail.PythonOperator(
            task_id='log_job_start_time',
            python_callable=lambda: now(
                config.time_zone).strftime("%Y-%m-%dT%H:%M:%S%z")
        )

        get_endpoint = rail.PythonOperator(
            task_id='get_endpoint',
            python_callable=lambda: custom_methods.get_endpoint_detail(config)
        )

        get_users_timeoff = rail.BambooHROperator(
            task_id='get_users_timeoff',
            company_domain="deltek",
            request_method='GET',
            endpoint="{{result('get_endpoint')}}",
            bamboohr_conn_id=config.bamboohr_conn_id,
            data_handler=custom_methods.filter_timeoff_by_type
        )

        # Write CSV with dedup key for each timeoff record
        write_csv_with_dedup_key = rail.WriteCSVFileOperator(
            task_id='write_csv_with_dedup_key',
            source=lambda: rail.result('get_users_timeoff'),
            header=config.reference_file_header,
            row=lambda item: custom_methods.reference_file_row_from_bamboohr(item, config.time_zone)
        )

        # Create collection from input data for querying
        create_collection_input_data = rail.CreateCollectionOperator(
            task_id='create_collection_input_data',
            source="{{ result('write_csv_with_dedup_key') }}",
            name="inputdata",
            columns=config.reference_file_columns
        )

        # Download reference file directly using fixed filename
        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=reference_file_fullpath
        )

        # Load CSV from reference file (returns empty if file doesn't exist)
        load_csv_from_reference_file = rail.LoadCSVFileOperator(
            task_id='load_csv_from_reference_file',
            document="{{ result('download_reference_file') or '' }}",
        )

        # Create collection from reference file (empty if no reference file)
        create_collection_reference_data = rail.CreateCollectionOperator(
            task_id='create_collection_reference_data',
            source="{{ result('load_csv_from_reference_file') or [] }}",
            name="referencedata",
            columns=config.reference_file_columns
        )

        # Query for new/changed records (dedup_key not in reference)
        query_new_changed_records = rail.QueryCollectionOperator(
            task_id='query_new_changed_records',
            name='newchangedrecords',
            query="""SELECT * FROM inputdata WHERE inputdata.dedup_key NOT IN
                (SELECT DISTINCT referencedata.dedup_key FROM referencedata WHERE referencedata.dedup_key != '')"""
        )

        # Combine reference records with new/changed records for the new reference file
        # Filter out blank records (where dedup_key is empty)
        query_combined_reference_data = rail.QueryCollectionOperator(
            task_id='query_combined_reference_data',
            query="""SELECT * FROM referencedata WHERE referencedata.dedup_key != ''
                UNION
                SELECT * FROM newchangedrecords WHERE newchangedrecords.dedup_key != ''"""
        )

        # Write combined data to CSV for reference file upload
        write_combined_reference_csv = rail.WriteCSVFileOperator(
            task_id='write_combined_reference_csv',
            source=lambda: rail.result('query_combined_reference_data'),
            header=config.reference_file_header,
            row=custom_methods.reference_file_row_from_collection
        )

        if_has_new_changed_records = rail.IfOperator(
            task_id='if_has_new_changed_records',
            test='''{{ result('query_new_changed_records', 'length') > 0 }}''',
            yes_task='get_enabled_timeoff_types',
            no_task='batch_end'
        )

        # Process new/changed records
        get_enabled_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        process_each_timeoff_from_bamboohr = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_timeoff_from_bamboohr',
            retries=0,
            items=custom_methods.get_new_changed_from_original,
            conf=lambda item: request_payload.process_each_timeoff_from_bamboohr(item, rail.result('get_enabled_timeoff_types')),
            trigger_dag_id=config.process_each_timeoff_record_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_process_each_timeoff_from_bamboohr_child = rail.WaitForDagRunsSensor(
            task_id='wait_process_each_timeoff_from_bamboohr_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_timeoff_from_bamboohr") }}'
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs='{{ result("process_each_timeoff_from_bamboohr") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            python_callable=custom_methods.do_format_logs
        )

        # Upload new reference file (overwrites existing)
        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content='''{{ result('write_combined_reference_csv') }}''',
            remote_filepath=reference_file_fullpath,
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

    # Task flow
    can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
    can_run_batch_task >> rail.Label("No") >> log_job_start_time

    # Get input data and download reference file
    log_job_start_time >> get_endpoint >> get_users_timeoff >> write_csv_with_dedup_key
    write_csv_with_dedup_key >> create_collection_input_data >> download_reference_file

    # Process reference file and compare
    download_reference_file >> load_csv_from_reference_file >> create_collection_reference_data
    create_collection_reference_data >> query_new_changed_records >> if_has_new_changed_records

    # No new/changed records - skip to end
    if_has_new_changed_records >> rail.Label("No") >> batch_end

    # Has new/changed records - process them and update reference file
    if_has_new_changed_records >> rail.Label("Yes") >> get_enabled_timeoff_types >> process_each_timeoff_from_bamboohr
    process_each_timeoff_from_bamboohr >> wait_process_each_timeoff_from_bamboohr_child
    wait_process_each_timeoff_from_bamboohr_child >> gather_child_logs >> format_logs
    format_logs >> query_combined_reference_data >> write_combined_reference_csv >> upload_new_reference_file >> batch_end

    return dag

rail.for_each_instance(create_main_airflow_dag)

import rail
from pendulum import datetime
from datetime import timedelta
from airflow.models import Variable
from transparentbpo.timeoff_import.utils import custom_methods


def create_cleanup_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.reference_file_cleanup_dag_id,
        description='TransparentBPO - TimeOff Reference File Cleanup',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2026, 4, 1, tz=config.time_zone),
        schedule_interval=config.reference_cleanup_schedule_interval,
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
                config.can_run_cleanup_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='download_reference_file'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='download_reference_file',
            end_task='batch_end',
        )

        # Download reference file
        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=reference_file_fullpath
        )

        if_has_reference_file = rail.IfOperator(
            task_id='if_has_reference_file',
            test=lambda: bool(rail.result('download_reference_file')),
            yes_task='load_csv_from_reference_file',
            no_task='batch_end'
        )

        # Load CSV from reference file
        load_csv_from_reference_file = rail.LoadCSVFileOperator(
            task_id='load_csv_from_reference_file',
            document="{{ result('download_reference_file') }}",
        )

        # Create collection from reference file
        create_collection_reference_data = rail.CreateCollectionOperator(
            task_id='create_collection_reference_data',
            source="{{ result('load_csv_from_reference_file') }}",
            name="referencedata",
            columns=config.reference_file_columns
        )

        # Get cutoff date for retention period
        get_cutoff_date = rail.PythonOperator(
            task_id='get_cutoff_date',
            python_callable=lambda: custom_methods.get_cutoff_date(
                config.time_zone, config.dedup_retention_days)
        )

        # Query to filter out records older than retention period (and blank records)
        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            query="""SELECT * FROM referencedata WHERE processed_date >= '{{ result('get_cutoff_date') }}' AND referencedata.dedup_key != ''"""
        )

        # Query to get records that will be deleted
        query_old_records = rail.QueryCollectionOperator(
            task_id='query_old_records',
            query="""SELECT * FROM referencedata WHERE processed_date < '{{ result('get_cutoff_date') }}'"""
        )

        if_has_records_to_delete = rail.IfOperator(
            task_id='if_has_records_to_delete',
            test='''{{ result('query_old_records', 'length') > 0 }}''',
            yes_task='write_cleaned_reference_csv',
            no_task='batch_end'
        )

        # Write cleaned data to CSV
        write_cleaned_reference_csv = rail.WriteCSVFileOperator(
            task_id='write_cleaned_reference_csv',
            source=lambda: rail.result('query_valid_records'),
            header=config.reference_file_header,
            row=custom_methods.reference_file_row_from_collection
        )

        # Upload cleaned reference file
        upload_cleaned_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_cleaned_reference_file',
            content='''{{ result('write_cleaned_reference_csv') }}''',
            remote_filepath=reference_file_fullpath,
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

    # Task flow
    can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
    can_run_batch_task >> rail.Label("No") >> download_reference_file

    download_reference_file >> if_has_reference_file

    # No reference file - nothing to clean
    if_has_reference_file >> rail.Label("No") >> batch_end

    # Has reference file - process cleanup
    if_has_reference_file >> rail.Label("Yes") >> load_csv_from_reference_file
    load_csv_from_reference_file >> create_collection_reference_data >> get_cutoff_date
    get_cutoff_date >> query_valid_records >> query_old_records >> if_has_records_to_delete

    # Has old records to delete - upload cleaned file
    if_has_records_to_delete >> rail.Label("Yes") >> write_cleaned_reference_csv >> upload_cleaned_reference_file >> batch_end

    # No old records - nothing to do
    if_has_records_to_delete >> rail.Label("No") >> batch_end

    return dag

rail.for_each_instance(create_cleanup_dag)

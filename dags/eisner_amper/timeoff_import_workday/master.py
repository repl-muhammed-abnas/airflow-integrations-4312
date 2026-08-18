from datetime import timedelta
from eisner_amper.timeoff_import_workday.utils import custom_methods, response_filters
from pendulum import now
import itertools
import rail

null = None

def create_dag(config):
    """
    Creates the main DAG for Eisner Amper TimeOff Import from Workday integration.
    
    This DAG orchestrates the complete time import process including file validation,
    CSV processing, record validation, and triggering child DAGs for individual processing.

    Args:
        config: Configuration module containing all instance-specific settings including
                DAG IDs, file paths, connection IDs, and processing parameters
    
    Returns:
        Airflow DAG: The configured master DAG for Eisner Amper time off import processing
    """
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'Eisner Amper Workday TimeOff Import - Master DAG ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        # Task: Monitor SFTP directory for new CSV files to process
        # Continuously checks the configured input directory for new CSV files
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        # Task: Record processing start time and generate unique log filename
        # Creates timestamp and filename for logging throughout the processing pipeline
        log_start_time_and_filename = rail.PythonOperator(
            task_id = "log_start_time_and_filename",
            python_callable=lambda: {
                "start_time": now().strftime(config.STANDARD_EMAIL_DATE_FORMAT),
                "log_filename": "log_"+ rail.render_template('{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_name | replace(".csv", "") }}_') + \
                    now().strftime("%Y%m%dT%H%M%S") + ".csv"
            }
        )

        # Task: Validate that the detected file has .csv extension
        # Ensures only CSV files are processed, rejecting other file formats
        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_csv_content',
            no_task='send_invalid_format_email',
        )

        # Task: Send email notification for non-CSV files
        # Notifies stakeholders when invalid file format is detected
        send_invalid_format_email = rail.EmailOperator(
            task_id='send_invalid_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon Workday TimeOff Import - Invalid Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/invalid_format_email.html"
        )

        # Task: Download the validated CSV file from SFTP server
        # Retrieves the file content for local processing
        download_csv_content = rail.SFTPDownloadFileOperator(
            task_id='download_csv_content',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        # Task: Check if file sensor successfully found a new file
        # Determines whether to proceed with archiving or clean up empty DAG run
        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        # Task: Clean up DAG run when no new files are found
        # Removes unnecessary DAG run entries to maintain clean execution history
        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        # Task: Move processed file to archive directory
        # Maintains file history and prevents reprocessing of the same file
        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=f"{config.archive_filepath}/{{{{ dag_run_ecid() | replace(':', '-')}}}}_{{{{ result('new_file_sensor') | file_name }}}}"
        )

        # Task: Parse CSV file content using configured delimiter
        # Loads CSV data with semicolon separator and UTF-8 encoding
        load_csv_data = rail.LoadCSVFileOperator(
            task_id='load_csv_data',
            document="{{ result('download_csv_content') }}",
            delimiter=config.csv_separator
        )

        # Task: Transform CSV data into queryable collection
        # Maps CSV columns to standardized field names for processing
        create_csv_collection = rail.CreateCollectionOperator(
            task_id='create_csv_collection',
            source="{{ result('load_csv_data') }}",
            name="time_import_records",
            columns=config.column_mapping
        )

        # Task: Verify that CSV parsing produced valid records
        # Ensures at least one record exists before proceeding with validation
        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('create_csv_collection', 'length') > 0 }}",
            yes_task='create_records_log',
            no_task='send_no_data_email'
        )

        # Task: Send email notification when file is empty
        # Alerts stakeholders that no data is available for processing
        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon Workday TimeOff Import - No Data - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_no_data_email.html"
        )

        # Task: Initialize logging system for record processing
        # Creates central log repository for tracking all processing activities
        create_records_log = rail.CreateLogOperator(
            task_id='create_records_log'
        )

        # Task: Validate records and split into valid/invalid
        # Performs comprehensive validation including date range and hours increment
        validate_and_split = rail.PythonOperator(
            task_id='validate_and_split',
            python_callable=custom_methods.validate_and_split_records
        )

        # Task: Determine if any records failed validation
        # Controls workflow branching based on presence of invalid data
        has_invalid_records = rail.IfOperator(
            task_id='has_invalid_records',
            test="{{ result('validate_and_split').has_invalid }}",
            yes_task='log_invalid_records',
            no_task='process_valid_records'
        )

        # Task: Log all validation errors for invalid records
        # Records specific validation failures for each rejected record
        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('create_records_log') }}",
            severity="Exception",
            items="{{ result('validate_and_split').validation_logs | load_json_artifact | to_json }}",
            message="Invalid time off import record",
            properties=lambda item: item  # Already formatted in validate_and_split_records
        )

        process_valid_records = rail.EmptyOperator(
            task_id='process_valid_records'
        )

        # Task: Determine if any records passed validation
        # Controls workflow branching based on presence of valid data
        has_valid_records = rail.IfOperator(
            task_id='has_valid_records',
            test="{{ result('validate_and_split').has_valid }}",
            yes_task='get_work_location_oef',
            no_task='trigger_log_generation'
        )

        get_work_location_oef = rail.RepliconServiceOperator(
            task_id='get_work_location_oef',
            endpoint="services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data=lambda: {
                "bindingContextUri": "urn:replicon:object-type:time-entry"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', config.WORK_LOCATION_OEF_NAME, 'uri')
        )

        get_work_location_oef_values = rail.RepliconServiceOperator(
            task_id='get_work_location_oef_values',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result("get_work_location_oef"),
            },
            data_handler=lambda response: rail.write_json_artifact(list(
                map(lambda item: {
                    "uri": item["uri"],
                    "name": item["name"]
                }, response.get("tags", [])))
            )
        )

        # Task: Extract unique employee IDs from valid records
        # Groups records by employee for efficient parallel processing
        get_unique_employee_ids = rail.PythonOperator(
            task_id='get_unique_employee_ids',
            python_callable=custom_methods.get_unique_employee_ids
        )

        # Task: Launch parallel child DAGs for employee processing
        # Distributes employee records across multiple parallel DAG runs
        trigger_process_users = rail.trigger_parallel_dagrun(
            task_id='trigger_process_users',
            trigger_dag_id=config.process_unique_users_child,
            items="{{ result('get_unique_employee_ids') | to_json }}",
            conf=lambda item: {
                "employee_id": item['employee_id'],
                "valid_records": rail.result('validate_and_split')['valid_records'],
                "work_location_oef_uri": rail.result("get_work_location_oef"),
                "work_location_oef_values": rail.result("get_work_location_oef_values")
            },
            parallel_count=config.process_parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Task: Collect DAG run IDs from all parallel child executions
        # Aggregates DAG run identifiers for subsequent log gathering
        get_trigger_process_users_dag_ids = rail.PythonOperator(
            task_id= 'get_trigger_process_users_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'trigger_process_users_{x+1}'), range(config.process_parallel_count))))),
            show_return_value_in_logs= False
        )

        # Task: Collect processing logs from all child DAG executions
        # Aggregates logs from parallel processing for comprehensive reporting
        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs='{{ result("get_trigger_process_users_dag_ids") }}',
            dagrun_task_id='create_process_user_log',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True
        )

        # Task: Launch log generation and reporting child DAG
        # Triggers final processing step for log consolidation and email notification
        trigger_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf=lambda: {
                'timeentrylogs': rail.result("gather_child_logs") if rail.result("gather_child_logs") else [],
                'otherlogs': rail.result("create_records_log"),
                'log_filename': rail.result("log_start_time_and_filename")["log_filename"],
                'start_time': rail.result("log_start_time_and_filename")["start_time"],
                'source_filename': rail.render_template('{{ result("new_file_sensor") | file_name }}'),
                'total_record_count': rail.result("create_csv_collection", key="length")
            }
        )

        # Define task dependencies and workflow execution order

        # Initial setup: File detection and timestamp logging
        new_file_sensor >> log_start_time_and_filename >> is_csv

        # CSV validation branch: File format validation and routing
        is_csv >> rail.Label("Yes") >> download_csv_content
        is_csv >> rail.Label("No") >> send_invalid_format_email

        # File processing: Download, content check, and archiving
        download_csv_content >> load_csv_data
        download_csv_content >> was_new_file_found
        was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("no") >> delete_this_dagrun

        # Data validation: CSV parsing and record count verification
        load_csv_data >> create_csv_collection >> has_any_records

        # Records check branches: Record existence validation and routing
        has_any_records >> rail.Label("Yes") >> create_records_log
        has_any_records >> rail.Label("No") >> send_no_data_email

        # Store and validate records: Mandatory field validation and separation
        create_records_log >> validate_and_split >> has_invalid_records

        has_invalid_records >> rail.Label("Yes") >> log_invalid_records >> process_valid_records
        has_invalid_records >> rail.Label("No") >> process_valid_records >> has_valid_records

        # Valid records processing: Employee grouping and parallel preparation
        has_valid_records >> rail.Label("Yes") >> get_work_location_oef
        has_valid_records >> rail.Label("No") >> trigger_log_generation

        # Parallel processing: Child DAG execution and log aggregation
        get_work_location_oef >> get_work_location_oef_values >> get_unique_employee_ids >> trigger_process_users
        trigger_process_users >> get_trigger_process_users_dag_ids >> gather_child_logs >> trigger_log_generation

    return dag


# Create DAGs for each instance
rail.for_each_instance(create_dag)
# master.py
from pendulum import now
import itertools
import rail
import os
from datetime import timedelta

# Import utilities
from tsystems.time_import_v2.utils import custom_methods, request_payload, response_filters
from tsystems.time_import_v2.task.get_prereqs import get_prereqs_task_group


def create_dag(config):
    """
    Creates the main DAG for T-Systems Time Import integration.
    
    This DAG orchestrates the complete time import process including file validation,
    CSV processing, record validation, and triggering child DAGs for individual processing.

    Args:
        config: Configuration module containing all instance-specific settings including
                DAG IDs, file paths, connection IDs, and processing parameters
    
    Returns:
        Airflow DAG: The configured master DAG for T-Systems time import processing
    """
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'T-Systems Time Import - Master DAG ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        # start_date=dt(2025,6,1, tz=config.timezone),
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
        log_start_time = rail.PythonOperator(
            task_id = "log_start_time",
            python_callable=lambda: {
                "start_time": now(config.timezone).isoformat(),
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
            cc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon Time Import - Invalid Format - {{ current_time_in_specified_tz() }}',
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

        # Task: Verify the downloaded file contains data
        # Helper function to check if CSV file has content before processing
        def check_file_content():
            """Check if the downloaded CSV file has any content.
            
            Returns:
                bool: True if file size > 0, False if empty
            """
            with rail.existing_artifact(rail.result('download_csv_content')) as artifact:
                return os.path.getsize(artifact.local_filename) > 0

        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=check_file_content,
            yes_task='load_csv_data',
            no_task='send_no_data_email'
        )

        # Task: Parse CSV file content using configured delimiter
        # Loads CSV data with semicolon separator and UTF-8 encoding
        load_csv_data = rail.LoadCSVFileOperator(
            task_id='load_csv_data',
            document="{{ result('download_csv_content') }}",
            delimiter=config.csv_separator,
            encoding="utf-8-sig"
        )

        # Task: Transform CSV data into queryable collection
        # Maps CSV columns to standardized field names for processing
        create_csv_collection = rail.CreateCollectionOperator(
            task_id='create_csv_collection',
            source="{{ result('load_csv_data') }}",
            name="raw_time_import_records",
            columns=config.column_mapping
        )

        query_add_row_number = rail.QueryCollectionOperator(
            task_id='query_add_row_number',
            query="SELECT ROW_NUMBER() OVER (ORDER BY ROWID) as row_number, * FROM raw_time_import_records",
            name='time_import_records'
        )

        # Task: Verify that CSV parsing produced valid records
        # Ensures at least one record exists before proceeding with validation
        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('query_add_row_number', 'length') > 0 }}",
            yes_task='create_records_log',
            no_task='send_no_data_email'
        )

        # Task: Send email notification when file is empty
        # Alerts stakeholders that no data is available for processing
        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            cc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon Time Import - No Data - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_no_data_email.html"
        )

        # Task: Initialize logging system for record processing
        # Creates central log repository for tracking all processing activities
        create_records_log = rail.CreateLogOperator(
            task_id='create_records_log'
        )

        process_records = rail.EmptyOperator(
            task_id='process_records'
        )

        # Task: Filter and store records with valid mandatory fields
        # Creates collection of records that passed validation for processing
        store_valid_records = rail.QueryCollectionOperator(
            task_id='store_valid_records',
            query="SELECT * FROM time_import_records WHERE NULLIF(reported_by, '') IS NOT NULL AND NULLIF(employee_id, '') IS NOT NULL AND NULLIF(entry_date, '') IS NOT NULL",
            name="valid_entries"
        )

        # Task: Filter and store records missing mandatory fields
        # Creates collection of invalid records for error reporting
        store_invalid_records = rail.QueryCollectionOperator(
            task_id='store_invalid_records',
            query="SELECT * FROM time_import_records WHERE NULLIF(reported_by, '') IS NULL OR NULLIF(employee_id, '') IS NULL OR NULLIF(entry_date, '') IS NULL",
            name="invalid_entries"
        )

        # Task: Extract every distinct non-blank employee ID in the file
        # Used to resolve usernames up front for log entries written before per-employee processing
        get_unique_employee_ids_all = rail.QueryCollectionOperator(
            task_id='get_unique_employee_ids_all',
            query="SELECT DISTINCT employee_id FROM time_import_records WHERE NULLIF(employee_id, '') IS NOT NULL",
            name="all_unique_employees"
        )

        # Task: Resolve usernames for every employee in the file in a single Replicon call.
        # Called unconditionally, including when get_unique_employee_ids_all is empty
        # (get_bulk_user_data_payload([]) produces an empty "users" list, which the DAG-wide
        # default trigger_rule none_failed_min_one_success on log_invalid_records tolerates
        # the same way any other successful-but-empty upstream would be).
        # Powers the Username column for log entries written before per-employee processing
        get_all_employees_user_details = rail.RepliconServiceOperator(
            task_id='get_all_employees_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: request_payload.get_bulk_user_data_payload(
                [e['employee_id'] for e in rail.load_all_records(rail.result('get_unique_employee_ids_all'))]
            ),
            data_handler=response_filters.build_employee_username_map
        )

        # Task: Log all validation errors for invalid records
        # Records specific validation failures for each rejected record
        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('create_records_log') }}",
            severity="Exception",
            items="{{ result('store_invalid_records') }}",
            message="Invalid time import record - missing mandatory field(s)",
            properties=lambda item: {
                'row_number': item['row_number'],
                'employee_id': item['employee_id'],
                'user_name': custom_methods.get_username_for_employee_id(item.get('employee_id')),
                'entry_date': item['entry_date'],
                'project_id': '',
                'task_name': '',
                'activity': '',
                'status': "Exception",
                'action': "Validation",
                'details': custom_methods.get_validation_error_message(item)
            }
        )

        # Task: Determine if any records passed validation
        # Controls workflow branching based on presence of valid data
        has_valid_records = rail.IfOperator(
            task_id='has_valid_records',
            test="{{ result('store_valid_records', 'length') > 0 }}",
            yes_task='get_reported_by_user_details',
            no_task='trigger_log_generation'
        )

        get_reported_by_user_details = rail.RepliconServiceOperator(
            task_id='get_reported_by_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: request_payload.get_user_data_payload(rail.load_all_records(rail.result('store_valid_records'))[0]['reported_by']),
            data_handler=lambda res: res[0] if len(
                res) > 0 and res[0]["userDetails"]["uri"] else None
        )

        if_reported_user_is_eligible_for_import = rail.IfOperator(
            task_id='if_reported_user_is_eligible_for_import',
            test=lambda: custom_methods.is_reported_by_user_eligible_for_time_import(config.time_import_eligibility_oef_name),
            yes_task='get_unique_employee_ids',
            no_task='log_ineligible_reportedby_records'
        )

        # Task: Log ineligible reported by user records
        # Records all ineligible reported by user entries for auditing
        log_ineligible_reportedby_records = rail.WriteLogOperator(
            task_id='log_ineligible_reportedby_records',
            log="{{ result('create_records_log') }}",
            severity="Exception",
            items="{{ result('store_valid_records') }}",
            message="Reported by user does not have the required permissions to perform the import",
            properties=lambda item: {
                'row_number': item['row_number'],
                'employee_id': item['employee_id'],
                'user_name': custom_methods.get_username_for_employee_id(item.get('employee_id')),
                'entry_date': item['entry_date'],
                'project_id': item['project_id'],
                'task_name': item['task_name'],
                'activity': item['activity'],
                'status': "Exception",
                'action': "Validation",
                'details': "Reported by user does not have the required permissions to perform the import"
            }
        )

        # Task: Extract unique employee IDs from valid records
        # Groups records by employee for efficient parallel processing
        get_unique_employee_ids = rail.QueryCollectionOperator(
            task_id='get_unique_employee_ids',
            query="SELECT DISTINCT Employee_ID FROM valid_entries WHERE Employee_ID IS NOT NULL",
            name="unique_employees"
        )

        # Task Group: Fetch prerequisite data for time entry processing
        # Retrieves billing rates, OEF definitions, and worktype configurations
        get_prereqsits_entry, get_prereqsits_exit = get_prereqs_task_group(config)

        # Task: Create configuration objects for each employee
        # Generates individual configurations for parallel child DAG execution
        prepare_parallel_configs = rail.PythonOperator(
            task_id='prepare_parallel_configs',
            python_callable=lambda: [
                {"employee_id": emp['employee_id']} 
                for emp in rail.load_all_records(rail.result('get_unique_employee_ids'))
            ]
        )

        # Task: Launch parallel child DAGs for employee processing
        # Distributes employee records across multiple parallel DAG runs
        trigger_process_users = rail.trigger_parallel_dagrun(
            task_id='trigger_process_users',
            trigger_dag_id=config.process_unique_users_child,
            items="{{ result('prepare_parallel_configs') | to_json }}",
            conf=lambda item: {
                "employee_id": item['employee_id'],
                "billing_rates": rail.result('get_enabled_company_billing_rates'),
                "worktype_oef": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_timeentry_oef_details'),
                    'name', config.worktype, 'uri'
                ),
                "worktype_oef_tags": rail.result('get_worktype_oef_details'),
                "worktype_tarif_oef": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_timeentry_oef_details'),
                    'name', config.worktype_tarif, 'uri'
                ),
                "worktype_tarif_oef_tags": rail.result('get_worktype_tarif_oef_details'),
                "worktype_tariffrei_oef": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_timeentry_oef_details'),
                    'name', config.worktype_tariffrei, 'uri'
                ),
                "worktype_tariffrei_oef_tags": rail.result('get_worktype_tariffrei_oef_details'),
                "reported_by_user_uri": rail.result('get_reported_by_user_details')["userDetails"]["uri"]
            },
            parallel_count=config.process_parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Task: Collect DAG run IDs from all parallel child executions
        # Aggregates DAG run identifiers for subsequent log gathering
        get_trigger_process_users_dag_ids =rail.PythonOperator(
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
                'timeentrylogs': rail.result("gather_child_logs") if rail.result("gather_child_logs") else None,
                'otherlogs': rail.result("create_records_log"),
                'log_filename': rail.result("log_start_time")["log_filename"],
                'start_time': rail.result("log_start_time")["start_time"],
                'input_filename': rail.render_template('{{ result("new_file_sensor") | file_name }}'),
                'reported_by_email': rail.result("get_reported_by_user_details")["userDetails"]["emailAddress"] if rail.result("get_reported_by_user_details") else None
            }
        )

        # Define task dependencies and workflow execution order
        
        # Initial setup: File detection and timestamp logging
        new_file_sensor >> log_start_time >> is_csv
        
        # CSV validation branch: File format validation and routing
        is_csv >> rail.Label("Yes") >> download_csv_content
        is_csv >> rail.Label("No") >> send_invalid_format_email
        
        # File processing: Download, content check, and archiving
        download_csv_content >> has_file_content
        download_csv_content >> was_new_file_found
        was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("no") >> delete_this_dagrun
        
        # Content check branches: Empty file detection and routing
        has_file_content >> rail.Label("Yes") >> load_csv_data
        has_file_content >> rail.Label("No") >> send_no_data_email
        
        # Data validation: CSV parsing and record count verification
        load_csv_data >> create_csv_collection >> query_add_row_number >> has_any_records
        
        # Records check branches: Record existence validation and routing
        has_any_records >> rail.Label("Yes") >> create_records_log >> process_records
        has_any_records >> rail.Label("No") >> send_no_data_email
        
        # Store and validate records: Mandatory field validation and separation
        process_records >> [store_valid_records, store_invalid_records, get_unique_employee_ids_all]

        # Bulk employee username lookup: Resolves usernames up front for log_invalid_records.
        # Called unconditionally (no if-branch/guard) - see get_all_employees_user_details's
        # definition above for why an empty employee list is safe to call BulkGetUsers3 with.
        get_unique_employee_ids_all >> get_all_employees_user_details

        [store_valid_records, store_invalid_records, get_all_employees_user_details] >> log_invalid_records >> has_valid_records
        
        # Valid records processing: Employee grouping and parallel preparation
        has_valid_records >> rail.Label("Yes") >> get_reported_by_user_details
        has_valid_records >> rail.Label("No") >> trigger_log_generation

        get_reported_by_user_details >> if_reported_user_is_eligible_for_import
        
        if_reported_user_is_eligible_for_import >> rail.Label("No") >> log_ineligible_reportedby_records >> trigger_log_generation
        if_reported_user_is_eligible_for_import >> rail.Label("Yes") >> get_unique_employee_ids
        
        # Parallel processing: Child DAG execution and log aggregation
        get_unique_employee_ids >> get_prereqsits_entry
        get_prereqsits_exit >> prepare_parallel_configs >> trigger_process_users
        trigger_process_users >> get_trigger_process_users_dag_ids >> gather_child_logs >> trigger_log_generation

    return dag


# Create DAGs for each instance
rail.for_each_instance(create_dag)
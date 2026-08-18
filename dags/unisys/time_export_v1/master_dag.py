"""
Unisys Fieldglass Time Export Integration - Master DAG
Orchestrates the overall time export process and triggers child DAGs for batch processing

Based on design document: Replicon to Fieldglass Integration - Technical Specification V1.1
"""
from datetime import datetime, timedelta
import itertools
import pendulum
import rail
from unisys.time_export_v1.utils.data_processing import get_report_payload, get_start_date, get_end_date
from unisys.time_export_v1.utils import custom_methods


def create_dag(config):
    """Create the Unisys Fieldglass time export master DAG
    
    This DAG orchestrates the complete time export process with enhanced data validation:
    - Retrieves Employee Pay and Timesheet Period reports from Replicon
    - Validates data availability and format for each report independently
    - Processes timesheet entries through parallel child DAGs
    - Consolidates results and triggers export generation child DAG
    - Handles no-data scenarios with appropriate email notifications
    """

    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description=f'Unisys Fieldglass Time Export - Master DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_run_master,
        schedule_interval=config.schedule_interval,
        start_date=pendulum.datetime(2025, 1, 1, tz=config.utc_timezone),
    ) as dag:

        # Task 1: Initialize master process logging
        # Creates a log entry to track the overall export process
        create_export_file_log = rail.CreateLogOperator(
            task_id='create_export_file_log'
        )

        def get_run_details():
            _now = pendulum.now()
            _timestamp = _now.strftime("%m%d%Y%H%M%S")
            return {
                'current_date': _now.strftime("%Y-%m-%d"),
                'log_filename': f"FUSION_{config.instance.upper()}_UNISYS_TS_"+  _timestamp + ".csv",
                'start_date': get_start_date(),
                'end_date': get_end_date(),
                'date_timestamp': _now.strftime("%Y-%m-%d") + "T" + _now.strftime("%H:%M:%S") + "Z"
            }

        # Task 2: Generate runtime details
        # Creates metadata for the current export run including date, filename, and DAG run configuration
        run_details = rail.PythonOperator(
            task_id='run_details',
            python_callable=get_run_details,
        )

        # Task 3: Get Employee Pay report metadata
        # Retrieves report details for Employee Pay Details report (primary data source)
        get_employee_pay_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_employee_pay_report_details',
            report_name=config.employee_pay_report_name,
        )

        # Task 4: Get Timesheet Period report metadata
        # Retrieves report details for Timesheet Period Details report (zero hours validation)
        get_timesheet_period_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_period_report_details',
            report_name=config.timesheet_period_report_name,
        )

        # Task 5: Generate Employee Pay report batch
        # Creates a batch job to generate the Employee Pay Details report
        generate_employee_pay_report_batch = rail.RepliconServiceOperator(
            task_id='generate_employee_pay_report_batch',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: get_report_payload('get_employee_pay_report_details')
        )

        # Task Group 6: Execute Employee Pay report batch
        # Executes the Employee Pay report generation and waits for completion
        execute_employee_pay_report_batch = rail.batch_execution(
            group_id='execute_employee_pay_report_batch',
            creation_task_id=generate_employee_pay_report_batch.task_id,
        )

        # Task 7: Retrieve Employee Pay report results
        # Gets the generated Employee Pay report data from Replicon
        get_employee_pay_report_batch_results = rail.RepliconServiceOperator(
            task_id="get_employee_pay_report_batch_results",
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={
                'reportGenerationBatchUri': "{{result('generate_employee_pay_report_batch')}}"},
        )

        # Task 8: Generate Timeseet Period report batch
        # Creates a batch job to generate the Timesheet Period Details report
        generate_timesheet_period_report_batch = rail.RepliconServiceOperator(
            task_id='generate_timesheet_period_report_batch',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: get_report_payload('get_timesheet_period_report_details')
        )

        # Task Group 9: Execute Timesheet Period report batch
        # Executes the Timesheet Period report generation and waits for completion
        execute_timesheet_period_report_batch = rail.batch_execution(
            group_id='execute_timesheet_period_report_batch',
            creation_task_id=generate_timesheet_period_report_batch.task_id,
        )

        # Task 10: Retrieve Timesheet Period report results
        # Gets the generated Timesheet Period report data from Replicon
        get_timesheet_period_report_batch_results = rail.RepliconServiceOperator(
            task_id="get_timesheet_period_report_batch_results",
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={
                'reportGenerationBatchUri': "{{result('generate_timesheet_period_report_batch')}}"},
        )
        
        # Task 11: Synchronization checkpoint
        # Ensures both Employee Pay and Timesheet Period reports are fully processed before validation
        wait_for_reports_to_process = rail.EmptyOperator(
            task_id='wait_for_reports_to_process'
        )

        # Task 12: Check for any report data availability
        # Primary conditional check: if either Employee Pay OR Timesheet Period reports have data,
        # proceed to individual report validation, otherwise send no-data notification
        has_any_report_data = rail.IfOperator(
            task_id='has_any_report_data',
            test=lambda: (not rail.result("get_employee_pay_report_batch_results")[
                'reportGenerationResults'][0]['payload'].startswith("No Data") or \
                    not rail.result("get_timesheet_period_report_batch_results")[
                'reportGenerationResults'][0]['payload'].startswith("No Data")),
            yes_task=["if_employee_pay_report_has_data", "if_timesheet_period_report_has_data"],
            no_task="log_no_data_to_export",
        )

        # Task 13: Individual Employee Pay report data validation
        # Checks specifically if Employee Pay report contains data, proceeds to column validation or skips to processing
        if_employee_pay_report_has_data = rail.IfOperator(
            task_id='if_employee_pay_report_has_data',
            test=lambda: not (rail.result("get_employee_pay_report_batch_results")[
                'reportGenerationResults'][0]['payload'].startswith("No Data")),
            yes_task="validate_employee_pay_report_columns",
            no_task="dummy_process_ts_entries",
        )

        # Task 14: Validate Employee Pay report structure
        # Ensures Employee Pay report has expected column headers before processing
        validate_employee_pay_report_columns = rail.IfOperator(
            task_id='validate_employee_pay_report_columns',
            test=lambda: rail.result("get_employee_pay_report_batch_results")['reportGenerationResults'][0]['payload'].startswith(
                config.expected_employee_pay_report_columns),
            yes_task="load_employee_pay_report_csv",
            no_task="fail_invalid_employee_pay_report_columns",
        )

        # Task 15: Load Employee Pay report CSV data
        # Parses the Employee Pay report CSV payload into structured data
        load_employee_pay_report_csv = rail.LoadCSVFileOperator(
            task_id="load_employee_pay_report_csv",
            document="{{ result('get_employee_pay_report_batch_results').reportGenerationResults[0].payload }}"
        )

        # Task 16: Create Employee Pay data collection
        # Creates a queryable collection from Employee Pay report data
        create_employee_pay_report_collection = rail.CreateCollectionOperator(
            task_id='create_employee_pay_report_collection',
            source="{{ result('load_employee_pay_report_csv') }}",
            name="employee_pay_data",
            columns=config.employee_pay_report_columns
        )

        # Task 17: Fail on invalid Employee Pay report columns
        # Terminates execution if Employee Pay report structure is incorrect
        fail_invalid_employee_pay_report_columns = rail.FailOperator(
            task_id='fail_invalid_employee_pay_report_columns',
            message="Employee Pay report columns do not match expected format. Expected: " + config.expected_employee_pay_report_columns,
        )

        # Task 18: Individual Timesheet Period report data validation
        # Checks specifically if Timesheet Period report contains data, proceeds to column validation or skips to processing
        if_timesheet_period_report_has_data = rail.IfOperator(
            task_id='if_timesheet_period_report_has_data',
            test=lambda: not (rail.result("get_timesheet_period_report_batch_results")[
                'reportGenerationResults'][0]['payload'].startswith("No Data")),
            yes_task="validate_timesheet_period_report_columns",
            no_task="dummy_process_ts_entries",
        )

        # Task 19: Validate Timesheet Period report structure
        # Ensures Timesheet Period report has expected column headers before processing
        validate_timesheet_period_report_columns = rail.IfOperator(
            task_id='validate_timesheet_period_report_columns',
            test=lambda: rail.result("get_timesheet_period_report_batch_results")['reportGenerationResults'][0]['payload'].startswith(
                config.expected_timesheet_period_report_columns),
            yes_task="load_timesheet_period_report_csv",
            no_task="fail_invalid_timesheet_period_report_columns",
        )

        # Task 20: Load Timesheet Period report CSV data
        # Parses the Timesheet Period report CSV payload into structured data
        load_timesheet_period_report_csv = rail.LoadCSVFileOperator(
            task_id="load_timesheet_period_report_csv",
            document="{{ result('get_timesheet_period_report_batch_results').reportGenerationResults[0].payload }}"
        )

        # Task 21: Create Timesheet Period data collection
        # Creates a queryable collection from Timesheet Period report data
        create_timesheet_period_report_collection = rail.CreateCollectionOperator(
            task_id='create_timesheet_period_report_collection',
            source="{{ result('load_timesheet_period_report_csv') }}",
            name="timesheet_period_data",
            columns=config.timesheet_period_report_columns
        )

        # Task 22: Fail on invalid Timesheet Period report columns
        # Terminates execution if Timesheet Period report structure is incorrect
        fail_invalid_timesheet_period_report_columns = rail.FailOperator(
            task_id='fail_invalid_timesheet_period_report_columns',
            message="Timesheet Period report columns do not match expected format. Expected: " + config.expected_timesheet_period_report_columns,
        )

        # Task 23: Query distinct timesheet URIs from Employee Pay data
        # Extracts unique timesheet period URIs that have actual time entries
        query_distinct_ts_uri_from_employee_pay = rail.QueryCollectionOperator(
            task_id='query_distinct_ts_uri_from_employee_pay',
            query="""SELECT distinct timesheet_period_uri from employee_pay_data WHERE NULLIF(timesheet_period_uri,'') IS NOT NULL AND
            approval_status='Approved'""",
            name='unique_ts_uris'
        )

        # Task 24: Identify zero hours timesheets
        # Finds timesheet periods with no actual hours (potential blank timesheets)
        query_zero_hours_timesheets = rail.QueryCollectionOperator(
            task_id='query_zero_hours_timesheets',
            query='''SELECT * FROM timesheet_period_data WHERE total_hrs='0.00' AND NULLIF(timesheet_period_uri,'') IS NOT NULL''',
            name='zero_hours_ts'
        )

        # Task 25: Log zero hour timesheets
        # Records details of timesheets with zero hours for audit trail
        log_zero_hour_ts = rail.WriteLogOperator(
            task_id='log_zero_hour_ts',
            log="{{ result('create_export_file_log') }}",
            message="Log 0 hour timesheets",
            items="{{ result('query_zero_hours_timesheets') }}",
            severity='Success',
            properties=lambda item: custom_methods.get_export_rows(item)
        )

        # Task 26: Handle no-data scenario with email notification
        # Sends notification email when neither Employee Pay nor Timesheet Period reports contain data
        log_no_data_to_export = rail.EmailOperator(
            task_id='log_no_data_to_export',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() + " | Time Export is completed with No Data to export - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/no_data_notification.html"
        )

        # Task 27: Dummy placeholder for timesheet processing
        # Placeholder task for flow control before triggering child DAGs
        dummy_process_ts_entries = rail.EmptyOperator(
            task_id='dummy_process_ts_entries'
        )

        # Task 28: Trigger parallel child DAGs for timesheet processing
        # Spawns multiple child DAGs to process timesheets in parallel batches
        process_ts_entries = rail.trigger_parallel_dagrun(
            task_id='process_ts_entries',
            items="{{ result('query_distinct_ts_uri_from_employee_pay') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_ts_entries,
            trigger_dag_id=config.process_entries,
            # conf=lambda item: {
            #     **item
            # },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Task 29: Collect child DAG run IDs
        # Aggregates all spawned child DAG run identifiers for result gathering
        get_process_ts_entries_dag_ids =rail.PythonOperator(
            task_id= 'get_process_ts_entries_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_ts_entries_{x+1}'), range(config.trigger_parallel_dagrun_count_process_ts_entries))))),
            show_return_value_in_logs= False
        )

        # Task 30: Gather results from child DAGs
        # Collects processing logs from all child DAG executions
        gather_entries_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_entries_logs',
            dag_runs='{{ result("get_process_ts_entries_dag_ids") }}',
            dagrun_task_id='create_entries_log',
            execution_timeout=timedelta(
                hours=config.gather_entries_logs_timeout_hours),
            flatten=True
        )

        # Task 31: Calculate total records from both reports
        # Combines record counts from Employee Pay and Timesheet Period reports
        calculate_total_records = rail.PythonOperator(
            task_id='calculate_total_records',
            python_callable=lambda: (
                int(rail.render_template("{{ result('create_employee_pay_report_collection', 'length') }}"))\
                     if rail.result('create_employee_pay_report_collection') else 0) +
                (int(rail.render_template("{{ result('create_timesheet_period_report_collection', 'length') }}"))\
                     if rail.result('create_timesheet_period_report_collection') else 0)
        )

        # Task 32: Trigger export generation child DAG
        # Initiates final log consolidation, CSV file generation, and SFTP upload process
        export_generation = rail.TriggerDagRunOperator(
            task_id='export_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.export_generation,
            conf={
                'entrieslogs': "{{ result('gather_entries_logs') }}",
                'otherlogs': "{{ result('create_export_file_log') }}",
                'log_filename': '{{ result("run_details").log_filename }}',
                'start_date': '{{ result("run_details").start_date }}',
                'end_date': '{{ result("run_details").end_date }}',
                'date_timestamp': '{{ result("run_details").date_timestamp }}',
                'total_records': '{{ result("calculate_total_records") }}',
                'base_employee_pay_report_data': '{{result("create_employee_pay_report_collection")}}',
                'base_timesheet_period_report_data': '{{result("create_timesheet_period_report_collection")}}'
            }
        )

        # ============================================================================
        # DAG TASK DEPENDENCIES (Flow Graph) - 29 Tasks Total
        # ============================================================================

        # Phase 1: Initialization and Setup (Tasks 1-2)
        # Task 1 → Task 2 → [Task 3, Task 4] (parallel branching)
        create_export_file_log >> run_details >> [get_employee_pay_report_details, get_timesheet_period_report_details]

        # Phase 2: Employee Pay Report Generation Chain (Tasks 3,5,6,7)
        # Task 3 → Task 5 → Task 6 → Task 7
        get_employee_pay_report_details >> generate_employee_pay_report_batch
        generate_employee_pay_report_batch >> execute_employee_pay_report_batch
        execute_employee_pay_report_batch >> get_employee_pay_report_batch_results

        # Phase 3: Timesheet Period Report Generation Chain (Tasks 4,8,9,10)
        # Task 4 → Task 8 → Task 9 → Task 10
        get_timesheet_period_report_details >> generate_timesheet_period_report_batch
        generate_timesheet_period_report_batch >> execute_timesheet_period_report_batch
        execute_timesheet_period_report_batch >> get_timesheet_period_report_batch_results

        # Phase 4: Synchronization and Data Availability Check (Tasks 7,10,11,12)
        # [Task 7, Task 10] → Task 11 → Task 12 (conditional branching)
        [get_employee_pay_report_batch_results, get_timesheet_period_report_batch_results] >> wait_for_reports_to_process >> has_any_report_data

        # Branch 1: Data Processing Path - Employee Pay Processing (Tasks 13-15)
        # Task 12 →(Yes)→ Task 13 →(Yes)→ Task 14 → Task 15
        has_any_report_data >> rail.Label("Yes") >> [if_employee_pay_report_has_data, if_timesheet_period_report_has_data]
        if_employee_pay_report_has_data >> rail.Label("Yes") >> validate_employee_pay_report_columns >> rail.Label("Yes") >> load_employee_pay_report_csv
        load_employee_pay_report_csv >> create_employee_pay_report_collection >> query_distinct_ts_uri_from_employee_pay

        query_distinct_ts_uri_from_employee_pay >> dummy_process_ts_entries

        if_employee_pay_report_has_data >> rail.Label("No") >> dummy_process_ts_entries

        # Branch 1 Continued: Timesheet Period Processing (Tasks 17-19)
        # Task 15 → Task 17 →(Yes)→ Task 18 → Task 19
        if_timesheet_period_report_has_data >> rail.Label("Yes") >> validate_timesheet_period_report_columns
        validate_timesheet_period_report_columns >> rail.Label("Yes") >> load_timesheet_period_report_csv
        load_timesheet_period_report_csv >> create_timesheet_period_report_collection

        if_timesheet_period_report_has_data >> rail.Label("No") >> dummy_process_ts_entries

        # Phase 5: Data Analysis and Zero Hours Detection (Tasks 21,23,24,25,27)
        # Task 21 → Task 23 → Task 24 → Task 25 → Task 27
        create_timesheet_period_report_collection >> query_zero_hours_timesheets
        query_zero_hours_timesheets >> log_zero_hour_ts >> dummy_process_ts_entries

        # Phase 6: Parallel Child DAG Processing and Export Generation (Tasks 27,28,29,30,31)
        # Task 27 → Task 28 → Task 29 → Task 30 → Task 31
        dummy_process_ts_entries >> process_ts_entries
        process_ts_entries >> get_process_ts_entries_dag_ids
        get_process_ts_entries_dag_ids >> gather_entries_logs
        gather_entries_logs >> calculate_total_records >> export_generation

        # Branch 2: No Data Available Path with Email Notification (Task 26)
        # Task 12 →(No)→ Task 26
        has_any_report_data >> rail.Label("No") >> log_no_data_to_export

        # Error Handling Branches for Data Format Validation (Tasks 17,22)
        # Task 14 →(No)→ Task 17 (Employee Pay column validation failure)
        # Task 19 →(No)→ Task 22 (Timesheet Period column validation failure)
        validate_employee_pay_report_columns >> rail.Label("No") >> fail_invalid_employee_pay_report_columns
        validate_timesheet_period_report_columns >> rail.Label("No") >> fail_invalid_timesheet_period_report_columns

        return dag

# Create DAG instances for each environment
rail.for_each_instance(create_dag)
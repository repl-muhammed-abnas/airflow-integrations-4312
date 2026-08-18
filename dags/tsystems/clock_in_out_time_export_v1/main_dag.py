"""
T-Systems Clock In/Out Export Master DAG

This DAG orchestrates the daily export of clock in/out data from Replicon to SAP HR.
It runs daily at 1:00 AM CET and exports clock in/out entries for all timesheets that
were submitted the previous day.

The set of timesheets to export is determined by the Timesheet Submission Data report
(submissions from the previous day, filtered in the report's UI). Clock in/out entries
are matched to those submissions on Employee ID + Timesheet Period.
"""

from pendulum import datetime as dt
import pendulum
import rail

from tsystems.clock_in_out_time_export_v1.utils import custom_methods

def create_dag(config):
    """
    Creates the master DAG for T-Systems Clock In/Out Export integration.
    
    This DAG coordinates the complete export process including:
    - Running the Clock In/Out report in Replicon
    - Running the Timesheet Submission Data report to identify timesheets
      submitted the previous day
    - Keeping only clock in/out entries whose timesheet was submitted
      (matched on Employee ID + Timesheet Period)
    - Processing and transforming the data
    - Exporting to JSON format
    - Uploading to SAP BTP via SFTP/API
    - Logging to Sumo Logic
    - Email notifications - Sending success/empty export emails
    
    Args:
        config: Configuration module with instance-specific settings
    
    Returns:
        Airflow DAG: The configured master DAG
    """
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'T-Systems Clock In/Out Time Export - Master DAG - {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2025,6,1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        # Record processing start time
        process_start_time = rail.PythonOperator(
            task_id='process_start_time',
            python_callable=lambda: pendulum.now(config.timezone).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        )

        # Generate export filename with timestamp
        generate_filename = rail.PythonOperator(
            task_id='generate_filename',
            python_callable=lambda: custom_methods.generate_export_filename(
                config.file_prefix,
                config.company_code,
                config.timestamp_format
            )
        )

        # Get Clock In/Out report details
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.clock_in_out_report_name
        )

        # Run the Clock In/Out Export report
        generate_clock_in_out_report = rail.run_report2(
            group_id='generate_clock_in_out_report',
            report_params=lambda:{
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        # Check if report generation failed
        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("generate_clock_in_out_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        # Handle report generation failure
        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="Clock In/Out report generation failed: {{result('generate_clock_in_out_report.get_report_result').reportGenerationResults[0].error}}"
        )

        # Check if report has data
        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('generate_clock_in_out_report.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='process_end_time',
        )

        # Load report data from CSV
        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('generate_clock_in_out_report.get_report_result').reportGenerationResults[0].payload }}",
            delimiter= ";"
        )

        # Create collection for data processing
        create_report_collection = rail.CreateCollectionOperator(
            task_id='create_report_collection',
            source='{{ result("load_report_data") }}',
            name='clock_data',
            columns={
                'Employee ID': 'employee_id',
                'Personal Number': 'personal_number',
                'Entry Date': 'entry_date',
                'Time In': 'clock_in',
                'Time Out': 'clock_out',
                'Hrs': 'hours',
                'WorkType HR200': 'worktype_hr200',
                'WorkType HR200 (Code)': 'worktype_hr200_code',
                'WorkType HR200 Tarif': 'worktype_hr200_tarif',
                'WorkType HR200 Tarif (Code)': 'worktype_hr200_tarif_code',
                'WorkType HR200 Tariffrei': 'worktype_hr200_tariffrei',
                'WorkType HR200 Tariffrei (Code)': 'worktype_hr200_tariffrei_code',
                'Approval Date': 'approval_date',
                'Approval Status': 'approval_status',
                'Timesheet Period': 'timesheet_period',
            }
        )
        # Get Timesheet Submission Data report details
        get_submission_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_submission_report_details',
            report_name=config.timesheet_submission_data_report_name
        )

        # Run the Timesheet Submission Data report. The report's UI filter already
        # restricts it to timesheets submitted the previous day, so no filter values
        # are passed here.
        generate_submission_report = rail.run_report2(
            group_id='generate_submission_report',
            report_params=lambda:{
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_submission_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        # Check if submission report generation failed
        is_submission_report_failed = rail.IfOperator(
            task_id="is_submission_report_failed",
            test='{{result("generate_submission_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_submission_report_generation",
            no_task="submission_report_has_data"
        )

        # Handle submission report generation failure
        fail_submission_report_generation = rail.FailOperator(
            task_id="fail_submission_report_generation",
            message="Timesheet Submission Data report generation failed: {{result('generate_submission_report.get_report_result').reportGenerationResults[0].error}}"
        )

        # Check if submission report has data. No submissions the previous day means
        # nothing to export.
        submission_report_has_data = rail.IfOperator(
            task_id="submission_report_has_data",
            test="{{ result('generate_submission_report.get_report_result', 'has_data') }}",
            yes_task='load_submission_data',
            no_task='process_end_time',
        )

        # Load submission report data from CSV
        load_submission_data = rail.LoadCSVFileOperator(
            task_id='load_submission_data',
            document="{{ result('generate_submission_report.get_report_result').reportGenerationResults[0].payload }}",
            delimiter= ";"
        )

        # Create collection of timesheets submitted the previous day
        create_submission_collection = rail.CreateCollectionOperator(
            task_id='create_submission_collection',
            source='{{ result("load_submission_data") }}',
            name='submission_data',
            columns={
                'Employee ID': 'employee_id',
                'Timesheet Period': 'timesheet_period',
                'Submitted On': 'submitted_on',
            }
        )

        # Query valid clock records: keep only clock in/out entries whose timesheet
        # was submitted the previous day, matched on employee id + timesheet period
        # against the submission report.
        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            query="""SELECT * FROM clock_data
                WHERE NULLIF(employee_id, '') IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM submission_data
                    WHERE submission_data.employee_id = clock_data.employee_id
                    AND submission_data.timesheet_period = clock_data.timesheet_period
                )""",
        )

        # Check if we have valid records after filtering
        has_valid_records = rail.IfOperator(
            task_id='has_valid_records',
            test='{{ result("query_valid_records", "length") > 0 }}',
            yes_task='process_clock_export_data',
            no_task='process_end_time'
        )

        # Process and transform clock data
        process_clock_export_data = rail.PythonOperator(
            task_id='process_clock_export_data',
            python_callable=custom_methods.process_clock_records,
        )

        # Generate JSON export file
        create_json_export = rail.PythonOperator(
            task_id='create_json_export',
            python_callable=lambda: custom_methods.create_json_export(
                rail.result('process_clock_export_data'),
                rail.result('generate_filename'),
                config
            )
        )

        # Get Access Token for API
        get_access_token = rail.SimpleHttpOperator(
            task_id='get_access_token',
            http_conn_id=config.client_http_conn_id,
            method='POST',
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            data='grant_type=client_credentials',
        )

        # Post file info to SAP BTP
        post_to_client_api = rail.SimpleHttpOperator(
            task_id='post_to_client_api',
            http_conn_id=config.client_post_api_http_conn,
            method='POST',
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer "+"{{ result('get_access_token') | from_json | attr_or_default('access_token', 'none') }}"
            },
            data='{{ result("create_json_export") }}',
            extra_options={
                'verify': False
            }
        )

        # Record processing end time
        process_end_time = rail.PythonOperator(
            task_id='process_end_time',
            python_callable=lambda: pendulum.now(config.timezone).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        )

        # Send success email
        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Clock In/Out Time Export Completed | {{ result("process_end_time") }}',
            html_content='templates/emails/export_complete.html',
            params={
                'export_date': pendulum.now(config.timezone).strftime(config.date_format_export)
            }
        )

        # Send empty export email
        send_empty_export_email = rail.EmailOperator(
            task_id='send_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Clock In/Out Time Export - No Data | {{ result("process_end_time") }}',
            html_content='templates/emails/export_empty.html',
            params={
                'export_date':pendulum.now(config.timezone).strftime(config.date_format_export),
                'report_name': config.clock_in_out_report_name,
            }
        )

        # Log to Sumo Logic
        log_to_sumo = rail.SendToSumoOperator(
            task_id='log_to_sumo',
            data={
                'job_start_time': "{{result('process_start_time')}}",
                'export_date': pendulum.now(config.timezone).strftime(config.date_format_export),
                'export_filename': "{{result('generate_filename')}}",
                'number_of_records': "{{ result('query_valid_records', 'length') or 0 }}",
                'export_status': "{{ 'success' if result('post_to_client_api') else 'failed' }}",
                'report_name': config.clock_in_out_report_name
            },
            sumo_conn_id='sumologic-exportlogger'
        )


        # Define task dependencies
        process_start_time >> generate_filename >> get_report_details

        get_report_details >> generate_clock_in_out_report >> is_report_failed

        is_report_failed >> rail.Label('Yes') >> fail_report_generation
        is_report_failed >> rail.Label('No') >> report_has_data

        report_has_data >> rail.Label('Yes') >> load_report_data >> create_report_collection
        report_has_data >> rail.Label('No') >> process_end_time >> send_empty_export_email

        # After the clock report is loaded, run the submission report to find out
        # which timesheets were submitted the previous day.
        create_report_collection >> get_submission_report_details >> generate_submission_report >> is_submission_report_failed

        is_submission_report_failed >> rail.Label('Yes') >> fail_submission_report_generation
        is_submission_report_failed >> rail.Label('No') >> submission_report_has_data

        submission_report_has_data >> rail.Label('Yes') >> load_submission_data >> create_submission_collection >> query_valid_records
        submission_report_has_data >> rail.Label('No') >> process_end_time >> send_empty_export_email

        query_valid_records >> has_valid_records

        has_valid_records >> rail.Label('Yes') >> process_clock_export_data >> create_json_export
        has_valid_records >> rail.Label('No') >> process_end_time >> send_empty_export_email

        create_json_export >> get_access_token >> post_to_client_api >> process_end_time >> send_success_email
        post_to_client_api >> process_end_time >> send_success_email >> log_to_sumo

    return dag


# Create DAGs for each instance
rail.for_each_instance(create_dag)

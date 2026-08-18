from datetime import timedelta
from pendulum import datetime
import pendulum
import rail

from mercury_systems_inc.time_export_daily.utils.python_callable import get_csv_filename, get_logging_details, format_csv_row, get_report_param

null=None

def create_dag(config):
    """
    Creates the daily time export DAG for Mercury Systems.
    
    This DAG runs daily at 11 PM and exports approved time entries with program name 'WO' 
    from the last 10 days, with approval date of today or yesterday.
    
    Args:
        config: Configuration object with DAG settings
        
    Returns:
        dag: Configured Airflow DAG
    """
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Mercury Systems Inc Daily Time Export {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 6, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        # Record start time of the process
        process_start_time = rail.PythonOperator(
            task_id='process_start_time',
            python_callable=lambda: pendulum.now(config.time_zone).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=get_logging_details,
            op_args=[config]
        )

        # Generate filename for the export
        generate_filename = rail.PythonOperator(
            task_id='generate_filename',
            python_callable=get_csv_filename,
            op_args=[config.time_zone]
        )

        
        # Get report details
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.export_report_name
        )

        get_wo_program_uri = rail.RepliconServiceOperator(
            task_id="get_wo_program_uri",
            endpoint="/services/ProgramListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:program-list-column:program"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:program-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "text": "WO"
                    },
                    "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
                },
                data_handler=lambda response:list(filter(lambda i: i["textValue"] == "WO", map (
                lambda i: i["cells"][0], response["rows"])) )[0]["uri"].split(":")[-1]
        )

        # Run report with filters
        report_group_entry, report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=get_report_param
        )

        # Check if report generation failed
        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        # Handle report generation failure
        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        # Check if report has data
        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result', 'has_data') }}",
            yes_task='report_has_expected_columns',
            no_task='log_to_sumo',
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            yes_task="load_report_data",
            no_task="fail_base_report_error"
        )

        fail_base_report_error = rail.FailOperator(
            task_id="fail_base_report_error",
            message="Base report error. Invalid column names."
        )

        # Load report data into memory
        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
            headers=["employee_id", "project_name", "project_id",
                "task_name", "task_id", "posting_date", "hours",
                "employee_approval", "manager_approval", "employee_ou",
                "employee_charge_type", "first_name", "last_name",
                "employee_department", "charge_type", "approval_day_diff", "time_entry_id"]
        )

        create_collection_report_data = rail.CreateCollectionOperator(
            task_id="create_colletion_report_data",
            source='{{result("load_report_data")}}',
            name="time_data"
        )

        query_data_within_range = rail.QueryCollectionOperator(
            task_id="query_data_within_range",
            query="""SELECT * FROM time_data WHERE (DATE(posting_date) BETWEEN DATE('{{ result("logging_details").entry_start_date }}') 
            AND DATE('{{ result("logging_details").entry_end_date }}')) OR 
            (NULLIF(approval_day_diff, "") IS NOT NULL AND
            CAST(approval_day_diff AS REAL) BETWEEN 0.0 AND 1.0)"""
        )

        if_data_within_range = rail.IfOperator(
            task_id="if_data_within_range",
            test='{{result("query_data_within_range","length") > 0}}',
            yes_task="write_time_data_to_csv",
            no_task="send_empty_export_email"
        )

        # Send email for empty exports
        send_empty_export_email = rail.EmailOperator(
            task_id="send_empty_export_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Daily Time Export - No records to export - {{ result("process_start_time") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'date_range': '{{ result("logging_details").entry_start_date }} to {{ result("logging_details").entry_end_date }}',
                'filename': '{{ result("generate_filename") }}',
                "report_name": config.export_report_name,
                "file_path":config.sftp_export_file_path
            }
        )

        # Format report data as CSV with required headers
        write_time_data_to_csv = rail.WriteCSVFileOperator(
            task_id='write_time_data_to_csv',
            source='{{ result("query_data_within_range") }}',
            header=[
                "EMPLOYEE ID",
                "PROJECT NAME",
                "WORK ORDER / PROJECT ID",
                "TASK NAME",
                "OPERATION / TASK ID",
                "POSTING DATE",
                "HOURS",
                "EMPLOYEE APPROVAL",
                "MANAGER APPROVAL",
                "EMPLOYEE OU",
                "EMPLOYEE CHARGE TYPE",
                "FIRST NAME",
                "LAST NAME",
                "EMPLOYEE DEPARTMENT",
                "CHARGE TYPE",
                "TIME ENTRY ID",
                "TIME OFF BOOKING ID"
            ],
            row=lambda item: format_csv_row(item),
            delimiter=',',
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv)
        )

        # Upload CSV file to SFTP
        upload_report_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_report_to_sftp',
            content='{{ result("write_time_data_to_csv") }}',
            remote_filepath=config.sftp_export_file_path + '/{{ result("generate_filename") }}'
        )

        # Send email for successful export
        send_valid_export_complete_email = rail.EmailOperator(
            task_id="send_valid_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Daily Time Export completed - {{ result("process_start_time") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.sftp_export_file_path
            }
        )

        # Log successful export to Sumo
        log_to_sumo = rail.SendToSumoOperator(
            task_id="log_to_sumo",
            data={
                'jobstarttime': '{{ result("process_start_time") }}',
                'jobendtime': '{{ current_time_in_specified_tz("UTC", "%Y-%m-%dT%H:%M:%S") }}',
                'exportperiod': '{{ result("logging_details").entry_start_date }} - {{ result("logging_details").entry_end_date }}',
                'exportfilename': '{{ result("generate_filename") }}',
                'exportfilepath': config.sftp_export_file_path,
                'numberofrecords': "{{ result('query_data_within_range', 'length')}}"
            },
            sumo_conn_id=config.sumo_conn_id
        )

        # Log DAG run to Sumo
        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_sumo_conn_id,
            extra_info=lambda: {
                'daterange': rail.result("logging_details")["entry_start_date"] + ' - ' + rail.result("logging_details")["entry_end_date"],
                'filename': rail.result("generate_filename"),
                'recordcount': rail.result('query_data_within_range', 'length')
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            trigger_rule="all_done",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dag"
        )

        fail_dag = rail.FailOperator(
            task_id="fail_dag",
            message="Daily report export failed"
        )

        # Define task dependencies
        process_start_time >> logging_details >> generate_filename >> get_report_details >>\
        get_wo_program_uri >> report_group_entry
        report_group_exit >> is_report_failed
        
        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data
        
        report_has_data >> rail.Label("Yes") >> report_has_expected_columns >> rail.Label("No") >> fail_base_report_error
        report_has_expected_columns >> rail.Label("Yes") >>\
        load_report_data >> create_collection_report_data >> query_data_within_range >>\
        if_data_within_range >> rail.Label("No") >> send_empty_export_email >> log_to_sumo
        if_data_within_range >> rail.Label("Yes") >>\
        write_time_data_to_csv >>\
        upload_report_to_sftp >> send_valid_export_complete_email >> log_to_sumo >> dagrun_log_to_sumo
        report_has_data >> rail.Label("No") >> log_to_sumo >>\
        dagrun_log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dag

    return dag


# Create DAG for each instance
rail.for_each_instance(create_dag)
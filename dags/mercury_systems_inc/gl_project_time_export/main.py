from datetime import timedelta
from pendulum import datetime,now
import rail
from mercury_systems_inc.gl_project_time_export.tasks.run_approval_date_report import run_approval_date_report_for_project_time
from mercury_systems_inc.gl_project_time_export.tasks.run_entry_date_report import run_entry_date_project_time_report
from mercury_systems_inc.gl_project_time_export.utils.python_callable import get_csv_filename, get_date_range, format_csv_row

null=None
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Mercury Systems Inc GL Project Time Export {config.instance}',
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
            python_callable=lambda: now(config.time_zone).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        )

        # Generate filename for the export
        generate_filename = rail.PythonOperator(
            task_id='generate_filename',
            python_callable=get_csv_filename,
            op_args=[config.time_zone]
        )

        get_date_range_values = rail.PythonOperator(
            task_id="get_date_range_values",
            python_callable=get_date_range
        )

        run_approval_date_report = run_approval_date_report_for_project_time(config)

        run_entry_date_report = run_entry_date_project_time_report(config)

        query_data_within_range = rail.QueryCollectionOperator(
            task_id="query_data_within_range",
            query="""SELECT * FROM cost_data WHERE NULLIF(week_ending,'') IS NOT NULL AND
                    NULLIF(time_in,'') IS NULL
                    UNION SELECT * FROM cost_approval_data WHERE NULLIF(week_ending,'') IS NOT NULL AND
                    NULLIF(time_in,'') IS NULL"""
        )

        if_data_within_range = rail.IfOperator(
            task_id="if_data_within_range",
            test='{{result("query_data_within_range", "length") > 0}}',
            yes_task="write_time_data_to_csv",
            no_task="send_empty_export_email"
        )

        # Send email for empty exports
        send_empty_export_email = rail.EmailOperator(
            task_id="send_empty_export_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Mercury Systems GL Project Time Export - No records to export - {{ result("process_start_time") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'date_range': '{{ result("get_date_range_values").start_date }} to {{ result("get_date_range_values").end_date }}',
                'filename': '{{ result("generate_filename") }}',
                "report_name": config.export_report_name,
                "file_path":config.sftp_export_file_path
            }
        )

        # Format report data as CSV with required headers
        write_time_data_to_csv = rail.WriteCSVFileOperator(
            task_id='write_time_data_to_csv',
            source='{{ result("query_data_within_range") }}',
            header=["Employee Name",
                    "Employee ID",
                    "Uses Activity",
                    "Union Code",
                    "Job Code",
                    "Pay Type",
                    "Week Ending",
                    "BU",
                    "Department",
                    "Activity",
                    "Weekly Hours",
                    "Weekly Earnings"],
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
            subject='{{ get_company_key() }} | GL Project Time Export completed - {{ result("process_start_time") }}',
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
                'jobendtime': '{{ current_time_in_specified_tz() }}',
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
                'filename': rail.result("generate_filename"),
                'recordcount': rail.result('query_data_within_range', 'length') if rail.result('query_data_within_range') else 0
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            trigger_rule="one_failed",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dag"
        )

        fail_dag = rail.FailOperator(
            task_id="fail_dag",
            message="GL Project Time Export failed"
        )

        # Define task dependencies
        process_start_time >>  generate_filename >> get_date_range_values>>\
        run_approval_date_report >> run_entry_date_report >>\
        query_data_within_range >> if_data_within_range >> rail.Label("No") >> send_empty_export_email
        if_data_within_range >> rail.Label("Yes") >> write_time_data_to_csv >>\
        upload_report_to_sftp >> send_valid_export_complete_email >> log_to_sumo >> dagrun_log_to_sumo
        dagrun_log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dag

    return dag


# Create DAG for each instance
rail.for_each_instance(create_dag)
from datetime import timedelta
from airflow.models import Variable
import rail
from tsystems.time_import_v1.utils import custom_methods

def create_child_dag(config):
    """
    Creates the Child DAG for log generation and reporting.
    
    This DAG handles the final processing step including log formatting,
    CSV report generation, file upload, and email notification.

    Args:
        config: Configuration module containing instance-specific settings,
                email configurations, and file path settings
    
    Returns:
        Airflow DAG: The configured child DAG for log generation and reporting
    """
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation,
        description=f'T-Systems Time Import Child - Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_log_gen_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Task: Check if batch processing mode is enabled
        # Controls execution flow for debugging vs production processing
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='format_logs'
        )

        # Task: Execute entry processing pipeline in batch mode
        # Wraps individual entry processing for monitoring and error handling
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='format_logs',
            end_task='batch_end',
        )

        # Task: Process and consolidate logs from all processing activities
        # Aggregates logs from record validation and time entry processing
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.do_format_logs,
            show_return_value_in_logs=False
        )

        # Task: Generate CSV report from processed log data
        # Creates structured CSV file with processing results for stakeholder review
        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'Employee ID',
                'Entry Date',
                'Project ID',
                'Task Name',
                'Activity',
                "Action",
                "Status",
                "Details",
                "ECID | Run ID",
            ],
            row=[
                "{{ item.employee_id }}",
                "{{ item.entry_date }}",
                "{{ item.project_id }}",
                "{{ item.task_name }}",
                "{{ item.activity }}",
                "{{ item.action }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.ecid }}",
            ]
        )

        # Get email details for notifications
        get_email_details = rail.PythonOperator(
            task_id='get_email_details',
            python_callable=lambda dag_run: custom_methods.get_email_details(
                config.timezone,
                config.log_filepath,
                dag_run
            )
        )

        # Task: Create secure download link for the generated log report
        # Generates time-limited URL for stakeholders to access processing results
        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{dag_run.conf.log_filename}}',
            expires_in_seconds=7*24*60*60,
        )

        # Task: Upload the log report to SFTP server for archival
        # Stores processing report in configured log directory for record keeping
        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/'+"{{dag_run.conf.log_filename}}",
        )

        # Task: Send completion email notification with processing summary
        # Notifies stakeholders of import completion with status and download link
        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to="{%- if dag_run.conf.reported_by_email | is_truthy -%}\
                    "+config.tenant_email+ ",{{ dag_run.conf.reported_by_email }}"+"\
                {%- else -%}\
                    "+config.tenant_email+"\
                {%- endif -%}",
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Time Import is " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/success_email.html"
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
        can_run_batch_task >> rail.Label("No") >> format_logs

        format_logs >> render_logs_csv >> generate_download_link >> get_email_details \
            >> upload_log_to_sftp >> send_import_complete_email >> batch_end

    return dag

rail.for_each_instance(create_child_dag)

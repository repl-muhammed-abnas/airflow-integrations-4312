"""T-Systems Time Import Child DAG for sending individual email notifications."""
from datetime import timedelta
from airflow.models import Variable
import rail

def create_child_dag(config):
    """
    Creates the Child DAG for sending email notifications to users and project managers.
    
    This DAG handles individual email notifications with filtered logs for each recipient.

    Args:
        config: Configuration module containing instance-specific settings
    
    Returns:
        Airflow DAG: The configured child DAG for email notifications
    """
    with rail.create_airflow_dag(
        dag_id=config.send_email_notification_child,
        description=f'T-Systems Jira Time Import Child - Send Email Notifications',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.send_email_max_active_runs_child,
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
            no_task='load_log_details'
        )

        # Task: Execute entry processing pipeline in batch mode
        # Wraps individual entry processing for monitoring and error handling
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='load_log_details',
            end_task='batch_end',
        )

        load_log_details = rail.PythonOperator(
            task_id='load_log_details',
            python_callable=lambda dag_run: rail.load_json_artifact(dag_run.conf['log_details'])
        )
        
        # Task: Generate CSV report from processed log data
        # Creates structured CSV file with processing results for stakeholder review
        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source='{{ result("load_log_details").logs | to_json }}',
            header=[
                'Unique ID',
                'Employee ID',
                'Entry Date',
                "Hours",
                'Project ID',
                'Task Name',
                "Action",
                "Status",
                "Details",
                "ECID | Run ID",
            ],
            row=[
                "{{ item.unique_id }}",
                "{{ item.employee_id }}",
                "{{ item.entry_date }}",
                "{{ item.hours }}",
                "{{ item.project_id }}",
                "{{ item.task_name }}",
                "{{ item.action }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.ecid }}",
            ]
        )

        # Task: Create secure download link for the generated log report
        # Generates time-limited URL for stakeholders to access processing results
        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name='{{ dag_run.conf.log_file_name }}',
            expires_in_seconds=7*24*60*60,
        )

        # Task: Check if we should upload logs to SFTP (only for all_logs notification type)
        check_upload_logs = rail.IfOperator(
            task_id='check_upload_logs',
            test=lambda dag_run: dag_run.conf.get('notification_type') == 'all_logs',
            yes_task='upload_log_to_sftp',
            no_task='send_import_complete_email'
        )

        # Task: Upload the log report to SFTP server for archival
        # Stores processing report in configured log directory for record keeping
        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/'+"{{ dag_run.conf.log_file_name }}",
        )

        # Task: Send completion email notification with processing summary
        # Notifies stakeholders of import completion with status and download link
        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to="{%- if dag_run.conf.notification_type == 'all_logs' -%}\
                    "+config.tenant_email+"\
                {%- else -%}\
                    {{ dag_run.conf.email }}\
                {%- endif -%}",
            bcc="{%- if result('load_log_details').error_count == 0 and dag_run.conf.notification_type == 'all_logs' -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    {%- if dag_run.conf.notification_type != 'all_logs' -%}\
                    ""\
                    {%- else -%}\
                    "+config.alert_email+"\
                    {%- endif -%}\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Jira Time Import is " }} \
                {%- if result("load_log_details").error_count > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("load_log_details").exception_count > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/import_complete.html",
            params={
                "log_file_path": config.log_filepath
            }
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
        can_run_batch_task >> rail.Label("No") >> load_log_details

        load_log_details >> render_logs_csv >> generate_download_link >> check_upload_logs
        check_upload_logs >> rail.Label("Yes") >> upload_log_to_sftp >> send_import_complete_email
        check_upload_logs >> rail.Label("No") >> send_import_complete_email

    return dag

rail.for_each_instance(create_child_dag)
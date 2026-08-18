from datetime import timedelta, datetime
import rail
from tsystems.project_import_v3.utils import custom_methods

def create_process_log_generation_dag(config):
    """
    Child DAG to process log generation and email notifications
    Formats logs, creates CSV, uploads to SFTP, and sends email
    """
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_dag_id,
        description='T-Systems Process Log Generation Child DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_second_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        # View DAG run configuration
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Format all logs collected during processing
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.format_integration_logs
        )

        # Create CSV from formatted logs
        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'projectid',
                'projectname', 
                'clientcode',
                'status',
                'action',
                'details',
                'ecid'
            ],
            row=[
                '{{ item.properties.projectid }}',
                '{{ item.properties.projectname }}',
                '{{ item.properties.clientcode }}',
                '{{ item.properties.status }}',
                '{{ item.properties.action }}',
                '{{ item.properties.details }}',
                '{{ item.ecid }}'
            ],
        )

        # Generate unique log file name with timestamp
        get_log_file_name = rail.PythonOperator(
            task_id='get_log_file_name',
            python_callable=lambda: f"{rail.get_company_key()}_project_import_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )

        # Upload logs to SFTP server
        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath + '/{{ result("get_log_file_name") }}.csv',
        )

        # Generate downloadable link for logs (expires in 7 days)
        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name="{{ result('get_log_file_name') }}.csv",
            expires_in_seconds=7*24*60*60 
        )

        # Send completion email with results
        send_completion_email = rail.EmailOperator(
            task_id='send_completion_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Project import - " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y/%m/%d/%H:%M:%S") }}',
            html_content="templates/email_import_complete.html",
        )

        # Define task dependencies
        format_logs >> render_logs_csv >> get_log_file_name >> upload_logs_to_sftp
        
        upload_logs_to_sftp >> generate_downloadable_link >> send_completion_email

    return dag

rail.for_each_instance(create_process_log_generation_dag)
"""
T-Systems Project Billing Rate Import - Log Pre-generation DAG

This DAG handles the pre-generation and formatting of logs before final processing.
"""

from datetime import timedelta
import rail
from tsystems.project_billing_rate_import.utils import custom_methods

# Required for JSON payload compatibility
null = None


def create_log_generation_dag(config):
    """
    Create the log generation DAG for processing logs.
    """

    with rail.create_airflow_dag(
        dag_id=config.log_generation_dag_id,
        description=f'T-Systems Project Billing Rate Import Log Generation Child {config.dag_id_suffix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dag_run_config'
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                "billing_rate_id",
                "billing_rate_name",
                "project_id",
                "ciam_id",
                "action",
                "status",
                "details",
                "jobid"
            ],
            row=[
                '{{ item.billing_rate_id }}',
                '{{ item.billing_rate_name }}',
                '{{ item.project_id }}',
                '{{ item.ciam_id }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.jobid }}'
            ],

            footer=[
                'Number of records found:{{ result("format_logs", key="total_record_count")}}',
                'Number of success records: {{ result("format_logs", key="success_record_count")}}',
                'Number of error records: {{ result("format_logs", key="error_record_count") }}',
                'Number of exception records: {{ result("format_logs", key="exception_record_count") }}',
                'Number of skipped records: {{ result("format_logs", key="skipped_record_count") }}',
            ]
        )

        get_email_and_log_file_details = rail.PythonOperator(
            task_id="get_email_and_log_file_details",
            python_callable=lambda dag_run: custom_methods.get_email_details_callable(
                dag_run, config.time_zone)
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name="{{result('get_email_and_log_file_details').log_file_name}}",
            expires_in_seconds=7*24*60*60,
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.sftp_log_filepath +
            "/{{result('get_email_and_log_file_details').log_file_name}}"
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0  -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Project Billing Rate Import - " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " | " + current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="templates/emails/completion_email.html",
            params={
                'log_filepath': config.sftp_log_filepath,
            }
        )

        format_logs >> render_logs_csv >> get_email_and_log_file_details >> generate_download_link >> upload_log_to_sftp >> send_import_complete_email

    return dag


# Create DAG instances for each environment
rail.for_each_instance(create_log_generation_dag)

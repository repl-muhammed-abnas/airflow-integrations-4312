from datetime import timedelta
import rail

from crl.office_schedule_import_v1.utils import custom_methods


def create_log_generation_dag(config):
    """
    Create DAG for generating integration logs
    """
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_dag_id,
        description=f'CRL Office Schedule Import Process Log Generation - {config.dag_id_suffix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_log_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Format and aggregate logs from master and child DAGs with statistics
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.format_all_logs,
            show_return_value_in_logs=False
        )

        # Generate CSV report from formatted logs
        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                "schedule_name",
                "pattern",
                "start_date",
                "action",
                "status",
                "details",
                "ecid",
            ],
            row=[
                "{{ item.schedule_name }}",
                "{{ item.pattern }}",
                "{{ item.start_date }}",
                "{{ item.action }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.jobid }}",
            ],
            footer=['Number of records found:{{ result("format_logs", key="total_record_count")}}',
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

        # Generate presigned download URL for CSV log (expires in 7 days)
        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name="{{result('get_email_and_log_file_details').log_file_name}}",
            expires_in_seconds=7*24*60*60,
        )

        # Upload log file to SFTP server
        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            "/{{ result('get_email_and_log_file_details').log_file_name }}",
        )

        # Send completion email with summary and download link
        send_completion_email = rail.EmailOperator(
            task_id='send_completion_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    " + config.internal_logs_email + "\
                {%- else -%}\
                    " + config.alert_email + "\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Office Schedule Import from SAP is " }} \
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
            html_content="templates/emails/completion_mail.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        format_logs >> render_logs_csv >> get_email_and_log_file_details >> generate_download_link >> upload_log_to_sftp >> send_completion_email

    return dag


rail.for_each_instance(create_log_generation_dag)

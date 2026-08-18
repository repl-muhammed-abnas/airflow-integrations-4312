"""
ViaPlus User Sync - Process Log Generation Child DAG

This child DAG handles the generation of CSV logs and email notifications.
It performs the following:
1. Format log records
2. Generate CSV log file
3. Upload to artifact storage
4. Generate downloadable link
5. Send email notification

Matches CRL user_import_ireland_v1 pattern.
"""
from datetime import timedelta
import rail

from viaplus.user_sync.utils.python_callable_methods import do_format_logs

null = None


def create_child_dag(config):
    """Create the process_log_generation child DAG."""

    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_dagid,
        description='ViaPlus User Sync - Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_log_generation,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # ================================================================
        # Step 1: Format logs from user processing
        # ================================================================
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs,
            show_return_value_in_logs=False
        )

        # ================================================================
        # Step 2: Generate CSV log file
        # ================================================================
        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'Employee ID',
                'Action',
                'Status',
                'Details',
                'Jobid',
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
            ],
            row=[
                '{{ item.employee_id }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.jobid }}'
            ],
            footer=[
                'Number of records found: {{ result("format_logs", key="total_record_count") }}',
                'Number of records processed: {{ result("format_logs", key="exception_record_count") + result("format_logs",key="error_record_count") + result("format_logs", key="success_record_count") }}',
                'Number of success records: {{ result("format_logs", key="success_record_count") }}',
                'Number of error records: {{ result("format_logs", key="error_record_count") }}',
                'Number of exception records: {{ result("format_logs", key="exception_record_count") }}',
            ]
        )

        # ================================================================
        # Step 3: Generate downloadable link
        # ================================================================
        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('render_logs_csv')}}",
            output_file_name="{{dag_run.conf.log_filename}}",
            expires_in_seconds=7 * 24 * 60 * 60  # 7 days
        )

        # ================================================================
        # Step 4: Send completion email
        # ================================================================
        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    " + config.internal_logs_email + "\
                {%- else -%}\
                    " + config.alert_email + "\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon User Sync " }} \
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
            html_content="templates/emails/import_complete_mail.html"
        )

        # ================================================================
        # Task Dependencies
        # ================================================================
        format_logs >> render_logs_csv >> generate_downloadable_link >> send_import_complete_email

    return dag


rail.for_each_instance(create_child_dag)

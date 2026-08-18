"""
Send Logs Child DAG for MichaelKors Timeoff Export to Workday

This DAG:
1. Receives aggregated logs from master DAG
2. Creates a CSV file from the log entries
3. Sends completion email with log file download link
"""
from datetime import timedelta
from airflow.models import Variable
import rail
from michaelkorstna.timeoff_export_to_workday.utils import custom_methods


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.send_logs_dag_id,
        description=f'MichaelKors Send Timeoff Export Logs {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='format_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='format_logs',
            end_task='finish',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Format and count logs
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            python_callable=custom_methods.format_logs,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Check if we have logs to process
        has_logs = rail.IfOperator(
            task_id='has_logs',
            test='{{ result("format_logs", key="total_count") > 0 }}',
            yes_task='create_log_csv',
            no_task='send_no_records_email'
        )

        # Create CSV from log entries (load from artifact)
        create_log_csv = rail.WriteCSVFileOperator(
            task_id='create_log_csv',
            source=lambda: rail.load_all_records(rail.result('format_logs')),
            header=[
                'Employee ID',
                'Login Name',
                'TimeOff Booking ID',
                'TimeOff Type Name',
                'TimeOff Code',
                'Hours',
                'Entry Date',
                'Transaction Type',
                'Status',
                'Details',
                'ECID'
            ],
            row=[
                '{{ item.employeeid }}',
                '{{ item.loginname }}',
                '{{ item.timeoffbookingid }}',
                '{{ item.timeofftypename }}',
                '{{ item.timeoffcode }}',
                '{{ item.hours }}',
                '{{ item.entrydate }}',
                '{{ item.transactiontype }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.ecid }}'
            ]
        )

        # Generate presigned download URL for email
        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("create_log_csv") }}',
            output_file_name='{{ dag_run.conf.filename }}',
            expires_in_seconds=7*24*60*60  # 7 days
        )

        # Send completion email with log file
        send_complete_email = rail.EmailOperator(
            task_id='send_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Timeoff Export to Workday is " }} \
                {%- if result("format_logs", key="error_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/success_email.html"
        )

        # Send email when no records to process
        send_no_records_email = rail.EmailOperator(
            task_id='send_no_records_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Timeoff Export to Workday - No Records - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/no_records_email.html"
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        # Task Dependencies
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> format_logs

        format_logs >> has_logs
        has_logs >> rail.Label('Yes') >> create_log_csv >> generate_download_link >> send_complete_email >> finish
        has_logs >> rail.Label('No') >> send_no_records_email >> finish

    return dag

rail.for_each_instance(create_dag)

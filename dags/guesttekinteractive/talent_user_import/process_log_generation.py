"""
Process Log Generation - GuestTek Talent User Import Child DAG
"""
from datetime import timedelta
import rail
from guesttekinteractive.talent_user_import.utils.custom_method import do_format_logs

null = None


def create_child_dag(config):
    """Create child DAG for generating and distributing import logs."""
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation,
        description='GuestTek Talent User Import - Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_log_generation,
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs,
            show_return_value_in_logs=False
        )
        
        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'Employee ID',
                'Action',
                'Status',
                'Details',
                'ECID',
            ],
            row=[
                '{{ item | attr_or_default("employee_id", "") }}',
                '{{ item | attr_or_default("action", "") }}',
                '{{ item | attr_or_default("status", "") }}',
                '{{ item | attr_or_default("details", "") }}',
                '{{ item | attr_or_default("ecid", "") }}',
            ]
        )
        
        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name='{{ dag_run.conf.log_filename }}',
            expires_in_seconds=config.log_file_download_link_expiry_in_sec,
        )
        
        send_completion_email = rail.EmailOperator(
            task_id='send_completion_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | GuestTek Talent User Sync - Completed - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/completion_mail.html"
        )
        
        format_logs >> render_logs_csv >> generate_download_link >> send_completion_email
    
    return dag


rail.for_each_instance(create_child_dag)

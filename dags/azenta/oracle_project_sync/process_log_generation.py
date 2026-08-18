"""
Log-generation child DAG - Azenta Oracle -> Polaris project sync.

Triggered once by the master after all per-project child DAGs finish. Renders the shared
run log to a CSV, generates a presigned download link for it, and emails a run report
(HTML template, subject reflecting success/exception/error counts) to the Azenta DL.

"""
from datetime import timedelta

import rail
from airflow.models import Variable

from azenta.oracle_project_sync.utils import custom_methods

# pylint: disable=expression-not-assigned,pointless-statement


def create_log_generation_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation,
        description=f'Azenta Oracle->Polaris log generation ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_log_gen_child,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_conf')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='format_logs',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='format_logs',
            end_task='batch_end',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Count entries by status (feeds the email subject/body).
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            python_callable=custom_methods.get_record_count_by_status,
            show_return_value_in_logs=False,
        )

        # Render the SQLite run log to a CSV artifact.
        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda dag_run: dag_run.conf['log'],
            header=[
                'Project ID',
                'Action',
                'Status',
                'Details',
                'ECID | Run ID',
            ],
            row=[
                "{{ item.properties.project_id | default('') }}",
                "{{ item.properties.action | default('') }}",
                "{{ item.properties.status | default('') }}",
                "{{ item.properties.details | default('') }}",
                "{{ item.properties.jobid | default(item.ecid) }}",
            ],
            footer=[
                'Number of success records: {{ result("format_logs", key="success_record_count") }}',
                'Number of error records: {{ result("format_logs", key="error_record_count") }}',
                'Number of exception records: {{ result("format_logs", key="exception_record_count") }}',
            ],
        )

        get_email_details = rail.PythonOperator(
            task_id='get_email_details',
            python_callable=lambda dag_run: custom_methods.get_log_email_details(
                config.timezone_iana, dag_run),
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name='{{ result("get_email_details")["log_file_name"] }}',
            expires_in_seconds=config.download_link_expiry_seconds,
        )

        send_sync_complete_email = rail.EmailOperator(
            task_id='send_sync_complete_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject=(
                "{{ get_company_key() }} | Polaris Sync {{ ds }} | "
                "{% if result('format_logs', key='error_record_count') > 0 %}completed with errors"
                "{% elif result('format_logs', key='exception_record_count') > 0 %}completed with exceptions"
                "{% else %}completed successfully"
                '{% endif %}'
            ),
            html_content='templates/emails/sync_complete_email.html',
        )

        batch_end = rail.EmptyOperator(task_id='batch_end')

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_end
        can_run_batch_task >> rail.Label('No') >> format_logs >> render_logs_csv \
            >> get_email_details >> generate_download_link >> send_sync_complete_email >> batch_end

        return dag


rail.for_each_instance(create_log_generation_dag)

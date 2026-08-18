from tsystems.timesheet_guessing_hours_update.utils import custom_methods
import rail
from datetime import timedelta
from airflow.models import Variable

null = None

def create_log_generation_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation,
        description=f"T-Systems Guessing Hours Update - Log Generation {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_log_gen_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='format_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='format_logs',
            end_task='batch_end',
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=custom_methods.do_format_logs,
            show_return_value_in_logs=False
        )

        # Task: Generate CSV report from processed log data
        # Creates structured CSV file with processing results for stakeholder review
        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source='{{ result("format_logs").logs | load_json_artifact | to_json }}',
            header=[
                'Employee ID',
                'Entry Date',
                'Org Structure Code',
                'Action',
                'Status',
                'Details',
                'ECID | Run ID',
            ],
            row=[
                "{{ item.employee_id }}",
                "{{ item.entry_date }}",
                "{{ item.org_structure_code }}",
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
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{dag_run.conf.log_filename}}',
            expires_in_seconds=7*24*60*60,
        )

        # Task: Send completion email notification with processing summary
        # Notifies stakeholders of import completion with status and download link
        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs').error_count == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Timesheet Guessing Hours Update is " }} \
                {%- if result("format_logs").error_count > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs").exception_count > 0 -%} \
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

        format_logs >> render_logs_csv >> generate_download_link >> send_import_complete_email >> batch_end

    return dag

rail.for_each_instance(create_log_generation_dag)
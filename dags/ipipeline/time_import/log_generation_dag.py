# Log Generation DAG for iPipeline Salesforce Integration

from datetime import timedelta
import rail

from ipipeline.time_import.utils import custom_methods


def create_log_generation_dag(config):
    """
    Create DAG for generating integration logs
    """
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_child_dag_id,
        description=f'iPipeline JIRA Time Import Process Log Generation - {config.dag_id_suffix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_log_generation_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

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
                'replicon_id',
                'task_type',
                'time_entry_date',
                'duration',
                'action',
                'status',
                'details',
                'jobid'
            ],
            row=[
                '{{ item.replicon_id }}',
                '{{ item.task_type }}',
                '{{ item.time_entry_date }}',
                '{{ item.hours}}',
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
            ]
        )

        get_email_and_log_file_details = rail.PythonOperator(
            task_id="get_email_and_log_file_details",
            python_callable=lambda dag_run: custom_methods.get_email_details_callable(
                dag_run, config.time_zone)
        )

        generate_download_link_for_logs = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link_for_logs',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name="{{result('get_email_and_log_file_details').log_file_name}}",
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Time Import from JIRA - " }} \
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
            html_content="templates/emails/import_complete_mail.html",
        )

        # Define workflow dependencies
        format_logs >> render_logs_csv >> get_email_and_log_file_details
        get_email_and_log_file_details >> generate_download_link_for_logs >> send_import_complete_email

    return dag


rail.for_each_instance(create_log_generation_dag)

from datetime import timedelta
from pendulum import now
import rail

from lead3rllc.project_import.utils.custom_methods import do_format_logs

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_dag_id,
        description='LEAD3R LLC Project Import - Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_log_generation,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs,
            show_return_value_in_logs=False
        )

        create_csv_lines_for_log_file = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_for_log_file',
            source="{{ result('format_logs') | to_json }}",
            header=['Deal ID',
                    'Deal Name',
                    'Company name',
                    'Action',
                    'Status',
                    'Details'],
            row=[
                "{{ item.deal_id }}",
                "{{ item.deal_name }}",
                "{{ item.company_name }}",
                "{{ item.action }}",
                "{{ item.status }}",
                "{{ item.details }}",
            ],

            footer=['Number of records found:{{ result("format_logs", key="total_record_count")}}',
                    'Number of records processed:'+'{{- result("format_logs", key="exception_record_count") + result("format_logs",key="error_record_count")+ \
                        result("format_logs", key="success_record_count")}}',
                    'Number of success records: {{ result("format_logs", key="success_record_count")}}',
                    'Number of error records: {{ result("format_logs", key="error_record_count") }}',
                    'Number of exception records: {{ result("format_logs", key="exception_record_count") }}',
                    ]
        )

        generate_downloadlink_logs = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_downloadlink_logs',
            artifact_name="{{ result('create_csv_lines_for_log_file')}}",
            output_file_name="project_import_log_{{ dag_run.conf.input_filename}}_{{ current_time_in_specified_tz('US/Pacific', '%Y-%m-%dT%H:%M:%S') }}.csv",
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() }} | Project import {{" "}} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- elif result("format_logs", key="exception_record_count") > 0 -%} \
                    completed with exceptions  \
                {%- else -%} \
                    completed Successfully - \
                {%- endif -%} \
                {{ " on " + current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}',
            html_content="/templates/import_complete.html"
        )

        format_logs >> create_csv_lines_for_log_file >> generate_downloadlink_logs >> send_import_complete_email

    return dag


rail.for_each_instance(create_child_dag)

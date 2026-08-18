from datetime import timedelta
import rail

from mercury_systems_inc.user_import.utils.custom_methods import get_email_details_callable
from mercury_systems_inc.user_import.utils.custom_methods import do_format_logs

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_dagid,
        description='MercurySystemsInc User Import Process Log Generation',
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

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'employee_id',
                'first_name',
                'last_name',
                'action',
                'status',
                'details',
                'jobid'
            ],
            row=[
                '{{ item.employee_id }}',
                '{{ item.first_name }}',
                '{{ item.last_name }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.jobid }}'
            ],

            footer=['Number of records found:{{ result("format_logs", key="total_record_count")}}',
                    'Number of records processed:'+'{{- result("format_logs", key="exception_record_count") + result("format_logs",key="error_record_count")+ \
                    result("format_logs", key="success_record_count")}}',
                    'Number of success records: {{ result("format_logs", key="success_record_count")}}',
                    'Number of error records: {{ result("format_logs", key="error_record_count") }}',
                    'Number of exception records: {{ result("format_logs", key="exception_record_count") }}',
                    ]
        )

        get_email_and_log_file_details = rail.PythonOperator(
            task_id="get_email_and_log_file_details",
            python_callable=lambda dag_run: get_email_details_callable(
                dag_run, config.time_zone)
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content='{{ result("render_logs_csv") }}',
            remote_filepath=config.sftp_log_filepath +
            "/{{result('get_email_and_log_file_details').log_file_name}}"
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon User Import from ADP HRIS - " }} \
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
            params={
                'log_filepath': config.sftp_log_filepath,
            }
        )

        format_logs >> render_logs_csv >> get_email_and_log_file_details >> upload_log_to_sftp >> send_import_complete_email

    return dag


rail.for_each_instance(create_child_dag)

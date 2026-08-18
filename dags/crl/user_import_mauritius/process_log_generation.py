from datetime import timedelta
import rail

from crl.user_import_mauritius.utils.python_callable_methods import do_format_logs

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_dagid,
        description='CRL User Import Mauritius - Process Log Generation',
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
                'Employee ID',
                'First Name',
                'Last Name',
                'Action',
                'Status',
                'Details',
                'Jobid',
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
            ],
            row=[   '{{ item.employee_id }}',
                    '{{ item.first_name }}',
                    '{{ item.last_name }}',
                    '{{ item.action }}',
                    '{{ item.status }}',
                    '{{ item.details }}', '{{ item.jobid }}'],

            footer=['Number of records found:{{ result("format_logs", key="total_record_count")}}',
                    'Number of records processed:'+'{{- result("format_logs", key="exception_record_count") + result("format_logs",key="error_record_count")+ \
                        result("format_logs", key="success_record_count")}}',
                    'Number of success records: {{ result("format_logs", key="success_record_count")}}',
                    'Number of error records: {{ result("format_logs", key="error_record_count") }}',
                    'Number of exception records: {{ result("format_logs", key="exception_record_count") }}',
                ]
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/'+"{{dag_run.conf.log_filename}}",
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('render_logs_csv')}}",
            output_file_name="{{dag_run.conf.log_filename}}",
            expires_in_seconds=7*24*60*60
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | User Import Mauritius " }} \
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
            html_content="templates/emails/import_complete_mail.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        format_logs >> render_logs_csv >> upload_log_to_sftp >> generate_downloadable_link >> send_import_complete_email

    return dag

rail.for_each_instance(create_child_dag)

from datetime import timedelta
import rail
from alvarezandmarsalholdings.rescind_user_import.utils.custom_methods import do_format_logs


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_dag_id,
        description=f'{config.company_key} Rescind User Import - Process Log Generation',
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
                "Employee ID",
                "Action",
                "Status",
                "Details",
                "JobID | Run ID",
            ],
            row=[
                "{{ item.employee_id }}",
                "{{ item.action }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.ecid }}",
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{dag_run.conf.log_filename}}',
            expires_in_seconds=7*24*60*60,
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/'+"{{dag_run.conf.log_filename}}",
        )

        if_log_contains_error_or_exception = rail.IfOperator(
            task_id='if_log_contains_error_or_exception',
            test='''{{ result("format_logs", key="error_record_count") > 0 or result("format_logs", key="exception_record_count") > 0 }}''',
            yes_task="send_import_complete_email"
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                "+config.internal_logs_email+"\
            {%- else -%}\
                "+config.alert_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() + " | Rescind User Import is " }} \
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
            html_content="templates/emails/completion_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                "total_records": '{{dag_run.conf.total_records}}',
                "success_records": '{{ result("format_logs", "success_record_count") }}',
                "exception_records": '{{ result("format_logs", "exception_record_count") }}',
                "failed_records": '{{ result("format_logs", "error_record_count") }}',
                "dag_run_id": '{{ dag_run_ecid() }}'
            },
        )

        format_logs >> render_logs_csv >> generate_download_link >> upload_log_to_sftp >> if_log_contains_error_or_exception >>\
            rail.Label('Yes') >> send_import_complete_email >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)

from datetime import timedelta
import rail
from mci.timeoff_sync.utils.python_callable import do_format_logs

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation,
        description=f'{config.company_key} MCI TimeOff Sync - Process Log Generation',
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
                "Employee_ID",
                "username",
                "status",
                "details",
                "ECID | Run ID",
            ],
            row=[
                "{{ item.Employee_ID }}",
                "{{ item.username }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.ecid }}",
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{dag_run.conf.filename}}',
            expires_in_seconds=7*24*60*60,
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/'+"{{dag_run.conf.filename}}",
        )

        failed_to_upload_sftp = rail.IfOperator(
            task_id="failed_to_upload_sftp",
            trigger_rule="all_done",
            test="{{ get_task_state('upload_log_to_sftp') | lower in ['failed', 'skipped'] }}",
            yes_task="send_sftp_upload_fail_email",
            no_task="send_import_complete_email"
        )

        send_sftp_upload_fail_email = rail.EmailOperator(
            task_id='send_sftp_upload_fail_email',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | TimeOff sync  to paycom - Uploading Logs to SFTP failed ' +
            '{{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/sftp_upload_file.html",
            params={
                'filename': "{{dag_run.conf.filename}}"
            },
            files=[
                ("{{ dag_run.conf.filename }}", "{{ result('render_logs_csv') }}")]
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                "+config.internal_logs_email+"\
            {%- else -%}\
                "+config.alert_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() + " | TimeOff sync from replicon to paycom " }} \
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

        format_logs >> render_logs_csv >> generate_download_link >> upload_log_to_sftp >> failed_to_upload_sftp
        failed_to_upload_sftp >> rail.Label("Yes") >> send_sftp_upload_fail_email
        failed_to_upload_sftp >> rail.Label("No") >> send_import_complete_email

    return dag

rail.for_each_instance(create_child_dag)

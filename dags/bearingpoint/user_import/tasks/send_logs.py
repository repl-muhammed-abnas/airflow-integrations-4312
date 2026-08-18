import pendulum
import rail

def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ result("format_log_records", "length") > 0 }}',
            yes_task='render_logs_csv'
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source='{{ result("format_log_records") }}',
            header=["Employee ID", "Action", "Status", "Details", "RunID"],
            row=lambda item:[
                item["employeeid"],
                item["action"],
                item["status"],
                item["details"],
                item["runid"]
            ]
        )

        get_log_filename = rail.PythonOperator(
            task_id='get_log_filename',
            python_callable=lambda: f'User_sync_logs_{pendulum.now(config.time_zone).strftime("%Y%m%dT%H%M%S")}.csv'
        )

        generate_log_file_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_log_file_link',
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name='{{ result("get_log_filename") }}',
            expires_in_seconds=config.log_file_link_expiry
        )

        upload_logs_to_api = rail.SimpleHttpOperator(
            task_id='upload_logs_to_api',
            method='POST',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data='{{ result("format_log_records") | load_all_records | to_json }}'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_log_records', key='get_logged_errors') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon User Sync from SAP to Replicon - " }} \
                {%- if result("format_log_records", key="get_logged_errors") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_log_records", key="get_logged_exceptions") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/import_complete.html"
        )

        has_any_entries_in_log >> rail.Label("Yes") >> render_logs_csv \
            >> get_log_filename >> generate_log_file_link >> upload_logs_to_api >> send_import_complete_email

        return has_any_entries_in_log, send_import_complete_email

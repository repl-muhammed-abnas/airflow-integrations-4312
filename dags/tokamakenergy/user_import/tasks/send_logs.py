from tokamakenergy.user_import.utils import custom_methods
import rail
import pendulum

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
            # pylint: disable=line-too-long
            header=["Username", "Employee ID", "Action", "Status", "Details", "ECID"],
            row=lambda item:[
                item["username"],
                item["employee_id"],
                item["action"],
                item["status"],
                item["comments"],
                item["ecid"]
            ]
        )

        get_log_filename = rail.PythonOperator(
            task_id='get_log_filename',
            python_callable=lambda: f'User_sync_log_{pendulum.now(config.time_zone).strftime("%Y%m%dT%H%M%S")}.csv'
        )

        generate_log_file_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_log_file_link',
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name='{{ result("get_log_filename") }}',
            expires_in_seconds=config.log_file_link_expiry
        )

        get_email_log_details = rail.PythonOperator(
            task_id='get_email_log_details',
            python_callable=custom_methods.get_email_log_details,
            op_args=[config.STANDARD_EMAIL_DATE_FORMAT]
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_log_records', key='get_logged_errors') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon User Sync from BambooHR to Polaris - " }} \
                {%- if result("format_log_records", key="get_logged_errors") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_log_records", key="get_logged_exceptions") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " | " + current_time_in_specified_tz() }}',
            html_content="templates/emails/import_complete.html"
        )

        has_any_entries_in_log >> rail.Label("Yes") >> render_logs_csv
        render_logs_csv >> get_log_filename >> generate_log_file_link >> get_email_log_details >> send_import_complete_email

        return has_any_entries_in_log, send_import_complete_email

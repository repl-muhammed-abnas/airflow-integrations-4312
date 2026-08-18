import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ result("create_log") | load_all_records() | length > 0 }}',
            yes_task=[
                'get_logged_errors',
                'get_logged_success', 'get_logged_exception'],
            no_task='fail_with_empty_log',
        )

        fail_with_empty_log = rail.FailOperator(
            task_id='fail_with_empty_log',
            message='No entries in log',
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            severity='Error',
            log="{{ result('create_log') }}"
        )

        get_logged_exception = rail.FilterLogEntriesOperator(
            task_id='get_logged_exception',
            severity='Exception',
            log="{{ result('create_log') }}"
        )

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            severity='Success',
            log="{{ result('create_log') }}"
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('create_log') }}",
            header=['LoginName', "Employee Type", "Group",
                    "Due Date", "Status", 'Details'],
            row=[
                '{{ item.properties.user_login_name }}',
                '{{ item.properties.employee_type }}',
                '{{ item.properties.group }}',
                '{{ item.properties.derived_due_date }}',
                '{{ item.properties.status }}',
                '{{ item.properties.details }}',
                '{{ item.ecid }}']

        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='log_{{ dag_run_ecid()}}_{{ dag_run.conf.file_name }}',
            expires_in_seconds=7*24*60*60,
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.alert_notification_log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-")}}_{{ dag_run.conf.file_name }}',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | General Notification for recovery enabled users is - " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exception", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%d%m%YT%H%M%S") }}',
            html_content="/templates/emails/email_notification_complete.html",
            params={
                'log_filepath': config.alert_notification_log_filepath,
            }
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_errors, get_logged_success, get_logged_exception] >>\
            render_logs_csv >> generate_download_link >> upload_log_to_sftp >> send_import_complete_email
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        return has_any_entries_in_log, send_import_complete_email

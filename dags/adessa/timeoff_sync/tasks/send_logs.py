import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ result("create_log") | load_all_records() | length > 0 }}',
            yes_task=['get_logged_errors', 'get_logged_success', 'get_logged_skipped'],
            no_task='fail_with_empty_log',
        )

        fail_with_empty_log = rail.FailOperator(
            task_id='fail_with_empty_log',
            message='No entries in log',
        )

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            severity='Success',
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            severity='Error',
        )

        get_logged_skipped = rail.FilterLogEntriesOperator(
            task_id='get_logged_skipped',
            severity='Skipped',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source='{{ result("create_log") }}',
            header=['Employee ID', 'Time  Off Type', 'Start Date', 'Action', 'Job ID', 'Details'],
            row=['{{ item.properties | attr_or_default("employeeid", "") }}', '{{  item.properties | attr_or_default("timeofftype", "") }}',
                 '{{ item.properties | attr_or_default("startdate", "") }}', '{{ item.properties | attr_or_default("action", "") }}',
                 '{{ dag_run_ecid() }}|{{ item.properties | attr_or_default("childjobid", "") }}', '{{ item.properties | attr_or_default("status", "") }}'],
            lineterminator='\n'
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath + '/Log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_name }}',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.internal_logs_email,
            bcc="{%- if result('get_logged_errors', 'length') > 0 -%}\
                "+config.alert_email+"\
            {%- else -%}\
                "+config.internal_logs_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() }} | Timeoff import {{" "}} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed Successfully - \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="/templates/emails/import_complete.html",
            params={
                'filepath': config.log_filepath
            }
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_errors, get_logged_success, get_logged_skipped] >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp >> send_import_complete_email
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        return has_any_entries_in_log, send_import_complete_email

import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ get_master_log() | load_all_records() | length > 0 }}',
            yes_task=['get_logged_errors',
                      'get_logged_exceptions', 'get_logged_add_success', 'get_logged_update_success', 'get_logged_skipped'],
            no_task='fail_with_empty_log',
        )

        fail_with_empty_log = rail.FailOperator(
            task_id='fail_with_empty_log',
            message='No entries in log',
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            severity='Error',
        )

        get_logged_exceptions = rail.FilterLogEntriesOperator(
            task_id='get_logged_exceptions',
            severity='Exception',
        )

        get_logged_add_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_add_success',
            severity='Add_Success',
        )

        get_logged_update_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_update_success',
            severity='Update_Success',
        )

        get_logged_skipped = rail.FilterLogEntriesOperator(
            task_id='get_logged_skipped',
            severity='Skipped',
        )

        filter_master_log = rail.FilterLogEntriesOperator(
            task_id='filter_master_log',
            log='{{ get_master_log() }}',
            severity='Pending_User',
            remove_filtered_entries=True
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            # pylint: disable=line-too-long
            header=['PersonID', 'FirstName', 'LastName', 'Status', 'Details', 'JobID',
                    '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                    'Number of Rows:'+ '{{- result("get_logged_errors", key="length") + \
                    result("get_logged_update_success", key="length")+ result("get_logged_add_success", key="length") + \
                    result("get_logged_exceptions", key="length") + result("get_logged_skipped", key="length") }}',
                    '', '', '', ''],
            row=['{{ item.properties | attr_or_default("employeeid", "") }}', '{{  item.properties | attr_or_default("firstname", "") }}',
                 '{{ item.properties | attr_or_default("lastname", "") }}', '{{ item.properties | attr_or_default("status", "") }}', '{{ item.message }}', '{{ item.ecid }}'],
            footer=['Number of records found:' + '{{- result("get_logged_exceptions", key="length") + result("get_logged_errors", key="length")+ result("get_logged_update_success", key="length")+ + result("get_logged_add_success", key="length") + result("get_logged_skipped", key="length")}}',
                    'Number of records processed:' + \
                    '{{- result("get_logged_exceptions", key="length") + result("get_logged_errors", key="length")+ result("get_logged_update_success", key="length")+ + result("get_logged_add_success", key="length")}}',
                    'Number of successes: {{- result("get_logged_add_success", "length")  + result("get_logged_update_success", "length")}}',
                    'Number of failures: {{ result("get_logged_errors", "length") }}',
                    'Number of users details added: {{ result("get_logged_add_success", "length") }}',
                    'Number of users details updated: {{ result("get_logged_update_success", "length") }}',
                    'Number of exceptions: {{ result("get_logged_exceptions", "length") }}',
                    'Number of Records Skipped/ignored: {{ result("get_logged_skipped", "length") }}', '', ''],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | User Sync - " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exceptions", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_errors, get_logged_exceptions, get_logged_add_success, get_logged_update_success, get_logged_skipped] >> filter_master_log
        filter_master_log >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp >> send_import_complete_email
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        return has_any_entries_in_log, send_import_complete_email

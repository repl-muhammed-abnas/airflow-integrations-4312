import rail
from nttdatabc.shift_automation.utils import python_callable_methods

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

        get_logged_add_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_add_success',
            severity='New User Shift Addition Success',
        )

        get_logged_update_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_update_success',
            severity='Existing User Shift Addition Success',
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            severity='Error',
        )

        get_logged_exceptions = rail.FilterLogEntriesOperator(
            task_id='get_logged_exceptions',
            severity='Exception',
        )

        get_logged_skipped = rail.FilterLogEntriesOperator(
            task_id='get_logged_skipped',
            severity='Skipped',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            # pylint: disable=line-too-long
            header=['Username', 'User Type', 'Shift Start Date', 'Shift End Date', 'Status', 'Details', 'ECID'],
            row=['{{ item.properties | attr_or_default("username", "") }}', '{{  item.properties | attr_or_default("usertype", "") }}',
                 '{{ item.properties | attr_or_default("shiftstartdate", "") }}', '{{ item.properties | attr_or_default("shiftenddate", "") }}',
                 '{{ item.properties | attr_or_default("status", "") }}', '{{ item.message }}', '{{ item.ecid }}'],
            footer=['Number of records found:' + '{{ result("get_logged_exceptions", key="length") + result("get_logged_errors", key="length") + result("get_logged_update_success", key="length") + result("get_logged_add_success", key="length") + result("get_logged_skipped", key="length")}}',
                    'Number of records processed:' + \
                    '{{ result("get_logged_exceptions", key="length") + result("get_logged_errors", key="length") + result("get_logged_skipped", key="length") + result("get_logged_update_success", key="length") + result("get_logged_add_success", key="length")}}',
                    'Number of users details added: {{ result("get_logged_add_success", "length") }}',
                    'Number of users details updated: {{ result("get_logged_update_success", "length") }}',
                    'Number of successes: {{ result("get_logged_add_success", "length") + result("get_logged_update_success", "length")}}',
                    'Number of failures: {{ result("get_logged_errors", "length") }}',
                    'Number of exceptions: {{ result("get_logged_exceptions", "length") }}',
                    'Number of skipped: {{ result("get_logged_skipped", "length") }}'],
        )

        get_log_filename = rail.PythonOperator(
            task_id='get_log_filename',
            python_callable=python_callable_methods.get_filename
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath + '/{{ result("get_log_filename") }}',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Shift Schedule Automation - " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exceptions", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - Uploading Logs to SFTP " + current_time_in_specified_tz() }}',
            html_content="/templates/emails/shift_assignment_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_errors, get_logged_exceptions, get_logged_add_success, get_logged_update_success, get_logged_skipped] >> render_logs_csv
        render_logs_csv >> get_log_filename >> upload_log_to_sftp >> send_import_complete_email
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        return has_any_entries_in_log, send_import_complete_email

import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ get_master_log() | load_all_records() | length > 0 }}',
            yes_task=['get_logged_errors',
                      'get_logged_exceptions', 'get_logged_success'],
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

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            severity='Success',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            # pylint: disable=line-too-long
            header=['Employee Id', 'Timeentry Id', 'Timesheet Status', 'Timesheet Period', 'Status', 'Details', 'JobID', '{{ current_time("%d/%m/%YT%H:%M:%S") }}', 'Number of Rows:' + '{{- result("get_logged_errors", key="length") + result("get_logged_success", key="length") + result("get_logged_exceptions", key="length") }}',
                    'Function: Adhoc Run - Time entry Submission', '', '', ''],
            row=['{{ item.properties | attr_or_default("Employee Id", "") }}', '{{  item.properties | attr_or_default("Timeentry Id", "") }}',
                 '{{ item.properties | attr_or_default("Timesheet Status", "") }}', '{{ item.properties | attr_or_default("Timesheet period", "") }}',
                 '{{ item.properties | attr_or_default("status", "") }}', '{{ item.message }}', '{{ item.ecid }}'],
            footer=['Number of Records Errored: {{ result("get_logged_errors", "length") }}',
                    'Number of Records Processed Successfully: {{ result("get_logged_success", "length") }}',
                    'Number of Records with Exception: {{ result("get_logged_exceptions", "length") }}', '', '', ''],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_errors, get_logged_exceptions, get_logged_success] >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        return has_any_entries_in_log

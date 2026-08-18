import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ get_master_log() | load_all_records() | length > 0 }}',
            yes_task=['get_logged_errors', 'get_logged_success'],
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

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            severity='Success',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=['Timesheet Period', 'Username', 'Status', 'Details', 'ECID'],
            row=['{{ item.properties | attr_or_default("timesheet_period", "") }}', '{{ item.properties | attr_or_default("username", "") }}',
                 '{{ item.properties | attr_or_default("status", "") }}', '{{ item.message }}', '{{ item.ecid }}']
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='logs_{{ dag_run_ecid() | replace(":", "-") }}_{{ current_time("%Y%m%dT%H%M%S") }}.csv',
            expires_in_seconds=config.s3_download_link_expiry,
        )

        send_complete_email = rail.EmailOperator(
            task_id='send_complete_email',
            to=config.internal_logs_email,
            bcc = "{%- if result('get_logged_errors', 'length') > 0  -%}\
                    "+config.alert_email+"\
                {%- else -%}\
                    "+config.internal_logs_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() }} | Timesheet auto population {{" "}} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors on  \
                {%- else -%} \
                    completed on  \
                {%- endif -%} \
                {{ " " + current_time_in_specified_tz() }}',
            html_content="/templates/emails/complete.html"
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [get_logged_errors, get_logged_success] >> render_logs_csv \
            >> generate_download_link >> send_complete_email
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        return has_any_entries_in_log, send_complete_email

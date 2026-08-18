import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ result("create_log") | load_all_records() | length > 0 }}',
            yes_task='get_logged_success',
            no_task='fail_with_empty_log',
        )

        fail_with_empty_log = rail.FailOperator(
            task_id='fail_with_empty_log',
            message='No entries in log',
        )

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            log='{{ result("create_log") }}',
            severity='Success',
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            log='{{ result("create_log") }}',
            severity='Error',
        )

        get_logged_skipped = rail.FilterLogEntriesOperator(
            task_id='get_logged_skipped',
            log='{{ result("create_log") }}',
            severity='Skipped',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('create_log') }}",
            header=['Username', 'Employee ID', 'Unique ID', 'Booking Start Date', 'Booking End Date', 'Status', 'Comments', 'ECID'],
            row=[
                '{{  item.properties | attr_or_default("username", "") }}',
                '{{ item.properties | attr_or_default("employee_id", "") }}',
                '{{ item.properties | attr_or_default("unique_id", "") }}',
                '{{ item.properties | attr_or_default("booking_start_date", "") }}',
                '{{ item.properties | attr_or_default("booking_end_date", "") }}',
                '{{ item.properties | attr_or_default("status", "") }}', 
                '{{ item.properties | attr_or_default("comments", "") }}',
                '{{ item.ecid }}'
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name="{{ result('logging_details').log_filename }}",
            expires_in_seconds=7*24*60*60
        )

        subject = '{{ get_company_key() + " | Timeoff Sync from HIBOB to Replicon is " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + result("logging_details").process_start_time }}'

        send_complete_email = rail.EmailOperator(
            task_id='send_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject=subject,
            html_content="/templates/emails/complete_sync.html"
        )

        has_any_entries_in_log >> rail.Label(
            "Yes") >> get_logged_success >> get_logged_errors >> get_logged_skipped >> render_logs_csv
        render_logs_csv >> generate_download_link >> send_complete_email
        has_any_entries_in_log >> rail.Label(
            "No") >> fail_with_empty_log

        return has_any_entries_in_log, send_complete_email

"""
Send Logs Module for ViaPlus Timeoff Sync Integration

This module creates a task group that handles log processing, CSV generation,
and email notifications with download links for the timeoff sync process.

Functions:
    get_send_logs: Creates and returns the send_logs task group
"""
import rail


def get_send_logs(config):
    """
    Create task group for processing and sending sync logs.
    
    This task group:
    1. Checks if any log entries exist
    2. Filters logs by severity (Success, Error, Skipped)
    3. Generates CSV file with all log entries
    4. Creates presigned download URL (valid 7 days)
    5. Sends completion email with download link
    
    Args:
        config: Instance configuration object
        
    Returns:
        tuple: (entry_task, exit_task) for DAG flow integration
    """
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        
        # Check if any log entries were created
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ result("create_log") | load_all_records() | length > 0 }}',
            yes_task='get_logged_success',
            no_task='fail_with_empty_log',
        )

        # Fail if no logs - indicates something went wrong
        fail_with_empty_log = rail.FailOperator(
            task_id='fail_with_empty_log',
            message='No entries in log',
        )

        # Filter log entries by severity
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

        # Generate CSV file with all log entries
        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('create_log') }}",
            header=[
                'Username',
                'Employee ID', 
                'Keka Booking ID',
                'Leave Type',
                'Start Date',
                'End Date',
                'Status',
                'Comments',
                'ECID'
            ],
            row=[
                '{{ item.properties | attr_or_default("username", "") }}',           # Changed from employee_name
                '{{ item.properties | attr_or_default("employee_id", "") }}',
                '{{ item.properties | attr_or_default("unique_id", "") }}',          # Changed from keka_booking_id
                '{{ item.properties | attr_or_default("leave_type", "") }}',         # This needs to be added to child DAGs
                '{{ item.properties | attr_or_default("booking_start_date", "") }}', # Changed from start_date
                '{{ item.properties | attr_or_default("booking_end_date", "") }}',   # Changed from end_date
                '{{ item.properties | attr_or_default("status", "") }}', 
                '{{ item.properties | attr_or_default("comments", "") }}',
                '{{ item.ecid }}'
            ]
        )

        # Generate presigned download URL (expires in 7 days)
        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name="{{ result('logging_details').log_filename }}",
            expires_in_seconds=7*24*60*60  # 7 days
        )

        # Build email subject with dynamic status
        subject = '{{ get_company_key() + " | Timeoff Sync from Keka to Replicon is " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + result("logging_details").process_start_time }}'

        # Send completion email with log download link
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

        # Task dependencies
        has_any_entries_in_log >> rail.Label("Yes") >> get_logged_success >> get_logged_errors >> get_logged_skipped >> render_logs_csv
        render_logs_csv >> generate_download_link >> send_complete_email
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        # Return entry and exit tasks for DAG flow integration
        return has_any_entries_in_log, send_complete_email
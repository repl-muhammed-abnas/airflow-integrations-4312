import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):

        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ result("create_log") | load_all_records() | length > 0 }}',
            yes_task=['get_logged_errors',
                      'get_logged_exceptions', 'get_logged_success', 'get_logged_skipped'],
            no_task='fail_with_empty_log',
        )

        fail_with_empty_log = rail.FailOperator(
            task_id='fail_with_empty_log',
            message='No entries in log',
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            severity='Failed',
        )

        get_logged_skipped = rail.FilterLogEntriesOperator(
            task_id='get_logged_skipped',
            severity='Skipped'
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
            source="{{ result('create_log') }}",
            header=['Project Code',
                    'Task Code',
                    'Status',
                    'Details',
                    'JobID'],
            row=['{{ item.properties.projectcode }}',
                 '{{ item.properties.taskcode }}',
                 '{{ item.properties.status }}',
                 '{{ item.properties.details }}',
                 '{{ dag_run_ecid() }}|{{ item.ecid }}']
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ result("new_file_sensor") | file_base }}_logs.csv',
            expires_in_seconds=7*24*60*60,
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath + '/{{ result("get_logging_details").log_filename }}'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Project Task import " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exceptions", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " on " + current_time_in_specified_tz() }}',
            html_content="templates/emails/import_complete.html"
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_errors, get_logged_exceptions, get_logged_success, get_logged_skipped] >> render_logs_csv
        render_logs_csv >> generate_download_link >> upload_log_to_sftp >> send_import_complete_email
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        return has_any_entries_in_log, send_import_complete_email

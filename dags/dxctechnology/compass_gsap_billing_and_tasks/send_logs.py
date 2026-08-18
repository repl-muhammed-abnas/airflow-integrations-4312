import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ get_master_log() | load_all_records() | length > 0 }}',
            yes_task=[
                'get_logged_errors',
                'get_logged_exceptions',
                'get_logged_success'],
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
            header=[
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                'Number of Rows: {{ result("query_merged_projects", "length") }}',
                'Function: GSAP Billing Key and Tasks',
                '',
                '',
                ''],
            row=['{{ item.properties.WBS }}', '{{ item.properties | attr_or_default("BillingKey", "") }}',
                 '{{ item.properties | attr_or_default("Task", "") }}',
                 '{{ item.severity }}', '{{ item.message }}', '{{ item.ecid }}'],
            footer=['Number of Records Errored: {{ result("get_logged_errors", "length") }}',
                    'Number of Records Processed Successfully: {{ result("get_logged_success", "length") }}',
                    'Number of Records with Exception: {{ result("get_logged_exceptions", "length") }}', '', '', ''],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id = 'upload_log_to_sftp',
            content = "{{ result('render_logs_csv') }}",
            remote_filepath = config.log_filepath + '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon task sync for Compass GSAP Billing and Tasks " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exceptions", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time() }}',
            html_content="email_import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_errors,
            get_logged_exceptions,
            get_logged_success] >> render_logs_csv >> upload_log_to_sftp >> send_import_complete_email
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log
        return has_any_entries_in_log, send_import_complete_email

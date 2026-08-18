import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ result("format_log_records", "length") > 0 }}',
            yes_task='render_logs_csv'
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source='{{ result("format_log_records") }}',
            # pylint: disable=line-too-long
            header=['{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                    'Number of Rows:' + '{{ result("format_log_records", key="get_logged_errors") + result("format_log_records", key="get_logged_success") +\
                      result("format_log_records", key="get_logged_exceptions") + result("format_log_records", key="get_logged_skipped")}}',
                    'Function: PSA Org Unit', '', '', ''],
            row=[
                    '{{ item | attr_or_default("organization_unit_cd", "") }}',
                    '{{ item | attr_or_default("status", "") }}',
                    '{{ item | attr_or_default("details", "") }}',
                    '{{ item.ecid }}'],
            footer=['Number of Records Errored: {{ result("format_log_records", key="get_logged_errors") }}',
                    'Number of Records Processed Successfully: {{ result("format_log_records", key="get_logged_success") }}',
                    'Number of Records with Exception: {{ result("format_log_records", key="get_logged_exceptions") }}',
                    'Number of Records Skipped: {{ result("format_log_records", key="get_logged_skipped") }}',
                    '', '', ''],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_log_records', key='get_logged_errors') == 0  -%}\
                    "+config.internal_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | PSA Organization Unit Import - " }} \
                {%- if result("format_log_records", key="get_logged_errors") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_log_records", key="get_logged_exceptions") > 0 -%} \
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

        has_any_entries_in_log >> rail.Label("Yes") >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp >> send_import_complete_email

        return has_any_entries_in_log, send_import_complete_email

import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ result("format_logs") | load_all_records() | length > 0 }}',
            yes_task=[
                'get_logged_errors', 'get_logged_skipped',
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
            source="{{ result('format_logs') | to_json }}",
            header=['UserName', "Employee Id",
                    "Action", 'Status', "Details"],
            row=[
                '{{ item.user_name }}',
                '{{ item.employee_id }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.ecid }}'
            ]

        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='log_{{ dag_run_ecid()}}_{{ result("new_file_sensor") | file_base | replace(".csv", "") }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        is_cost_center_log_generated = rail.IfOperator(
            task_id='is_cost_center_log_generated',
            test="{{ result('get_cost_center_log_file') | is_truthy }}",
            no_task="upload_log_to_sftp",
            yes_task='generate_download_link_for_costcenter_log'
        )

        generate_download_link_for_costcenter_log = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link_for_costcenter_log",
            artifact_name="{{ result('get_cost_center_log_file')[0]}}",
            output_file_name='costcenter_log_{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_base | replace(".csv", "")}}.csv',
            expires_in_seconds=7*24*60*60,
        )

        upload_cost_center_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_cost_center_log_to_sftp',
            content="{{ result('get_cost_center_log_file')[0]}}",
            remote_filepath=config.log_filepath +
            '/costcenter_log_{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_base | replace(".csv", "")}}.csv',
        )

        is_department_log_generated = rail.IfOperator(
            task_id='is_department_log_generated',
            test="{{ result('get_department_log_file') | is_truthy }}",
            no_task="upload_log_to_sftp",
            yes_task='generate_download_link_for_department_log'
        )

        generate_download_link_for_department_log = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link_for_department_log",
            artifact_name="{{ result('get_department_log_file')[0]}}",
            output_file_name='department_log_{{ dag_run_ecid()}}_{{ result("new_file_sensor") | file_base | replace(".csv", "") }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        upload_department_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_department_log_to_sftp',
            content="{{ result('get_department_log_file')[0]}}",
            remote_filepath=config.log_filepath +
            '/department_log_{{ dag_run_ecid()}}_{{ result("new_file_sensor") | file_base | replace(".csv", "") }}.csv',
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid()}}_{{ result("new_file_sensor") | file_base | replace(".csv", "") }}.csv',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | User import is - " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exceptions", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%d%m%YT%H%M%S") }}',
            html_content="/templates/emails/email_import_complete_user_import.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_errors, get_logged_skipped,
            get_logged_exceptions,
            get_logged_success] >> render_logs_csv >> generate_download_link >> [is_cost_center_log_generated, is_department_log_generated]\
            >> rail.Label("No") >> upload_log_to_sftp >> send_import_complete_email
        is_department_log_generated >> rail.Label("Yes") >> generate_download_link_for_department_log >> upload_department_log_to_sftp\
            >> upload_log_to_sftp
        is_cost_center_log_generated >> rail.Label("Yes") >> generate_download_link_for_costcenter_log >> upload_cost_center_log_to_sftp\
            >> upload_log_to_sftp
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        return has_any_entries_in_log, send_import_complete_email

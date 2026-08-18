import pendulum
import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ get_master_log() | load_all_records() | length > 0 }}',
            yes_task=['get_logged_errors', 'get_logged_success', 'get_logged_skipped'],
            no_task='fail_with_empty_log',
        )

        fail_with_empty_log = rail.FailOperator(
            task_id='fail_with_empty_log',
            message='No entries in log',
        )

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            severity='Success',
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            severity='Error',
        )

        get_logged_skipped = rail.FilterLogEntriesOperator(
            task_id='get_logged_skipped',
            severity='Skipped',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=['Name', 'Value', 'Status', 'Processing Status', 'Details', 'ECID'],
            row=['{{ item.properties | attr_or_default("name", "") }}', '{{  item.properties | attr_or_default("value", "") }}',
                 '{{ item.properties | attr_or_default("status", "") }}', '{{ item.properties | attr_or_default("processing_status", "") }}',
                 '{{ item.properties | attr_or_default("details", "") }}', '{{ item.ecid }}'],
        )

        get_filename = rail.PythonOperator(
            task_id='get_filename',
            python_callable=lambda: 'Offerings_Supported_Log_'+pendulum.now().strftime("%Y%m%d%H%M%S")+'.txt'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("render_logs_csv")}}',
            output_file_name='{{ result("get_filename") }}',
            expires_in_seconds=config.download_link_validity,
        )

        encrypt_file = rail.PGPEncryptionOperator(
            task_id='encrypt_file',
            source='{{ result("render_logs_csv")}}',
            pgp_conn_id=config.pgp_conn_id
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('encrypt_file') }}",
            remote_filepath=config.log_filepath + "/" + '{{ result("get_filename") }}' + '.pgp',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.internal_logs_email,
            bcc = "{%- if result('get_logged_errors', 'length') > 0  -%}\
                    "+config.alert_email+"\
                {%- else -%}\
                    "+config.internal_logs_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() }} | OEF Offerings Supported import to Replicon - {{" "}} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors on  \
                {%- else -%} \
                    completed successfully \
                {%- endif -%} \
                {{ " " + current_time("%m/%d/%Y") }}',
            html_content="/templates/emails/import_complete.html",
            params={
                'filepath': config.log_filepath
            }
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_errors, get_logged_success, get_logged_skipped] >> render_logs_csv
        render_logs_csv >> get_filename >> generate_download_link >> encrypt_file >> upload_log_to_sftp >> send_import_complete_email
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        return has_any_entries_in_log, send_import_complete_email

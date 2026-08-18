import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ result("format_logs") | load_all_records() | length > 0 }}',
            yes_task=[
                'get_logged_errors',
                'get_logged_ignored',
                'get_logged_success'],
            no_task='fail_with_empty_log',
        )

        fail_with_empty_log = rail.FailOperator(
            task_id='fail_with_empty_log',
            message='No entries in log'
        )

        def get_filtered_data(status):
            if list(filter(lambda item: item['status'].lower() == status, rail.result('format_logs'))):
                return len(list(filter(lambda item: item['status'].lower() == status, rail.result('format_logs'))))
            return 0

        get_logged_errors = rail.PythonOperator(
            task_id='get_logged_errors',
            python_callable=lambda: rail.set_result(
                get_filtered_data("error"), 'length')
        )

        get_logged_ignored = rail.PythonOperator(
            task_id='get_logged_ignored',
            python_callable=lambda: rail.set_result(
                get_filtered_data("ignored"), 'length')
        )

        get_logged_success = rail.PythonOperator(
            task_id='get_logged_success',
            python_callable=lambda: rail.set_result(
                get_filtered_data("success"), 'length')
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source='{{result("format_logs") | to_json}}',
            header=["User Name (guid)", "Action", "Status",
                    "Details", "JobID"],
            row=[
                '{{ item.guid }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.ecid }}'],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.user_import_log_path +
            '{{ dag_run_ecid() | replace(":", "-")}}_log_{{dag_run.conf.file_name}}',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " |  Australia -  User import - " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                        completed successfully  \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz("Australia/Sydney","%Y-%m-%dT%H%M%S%z") }}',
            html_content="templates/email/email_import_complete_user_import.html",
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_errors,
            get_logged_ignored,
            get_logged_success] >> render_logs_csv >> upload_log_to_sftp >> send_import_complete_email
        has_any_entries_in_log >> rail.Label("No") >> fail_with_empty_log

        return has_any_entries_in_log, send_import_complete_email

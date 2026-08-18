import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        send_logs_start = rail.EmptyOperator(
            task_id="send_logs_start"
        )
        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            log='{{ result("create_termination_log")}}',
            severity='Error',
        )
        get_logged_ignored = rail.FilterLogEntriesOperator(
            task_id='get_logged_ignored',
            log='{{ result("create_termination_log")}}',
            severity='ignored',
        )

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            log='{{ result("create_termination_log")}}',
            severity='Success',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source='{{ result("create_termination_log")}}',
            header=['employeeid', 'guid', 'status', 'details', 'jobid'],
            row=[
                '{{ item.properties.employeeid }}',
                '{{ item.properties.guid }}',
                '{{ item.properties.status }}',
                '{{ item.properties.details }}',
                '{{ item.ecid }}'],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.termination_details_log_filepath +
            '{{ dag_run_ecid() | replace(":", "-")}}'
            + "_PwCGlobal_usertermination_log_" +
            '{{current_time_in_specified_tz("Australia/Sydney","%m-%d-%Y")}}.csv',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " |  Australia - User termination job has - " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                        completed successfully  \
                {%- endif -%} \
                {{ " - " + current_time("%d%m%YT%H%M%S") }}',
            html_content="templates/email/email_import_complete_termination.html",
        )

        send_logs_start >> [
            get_logged_errors,
            get_logged_ignored,
            get_logged_success] >> render_logs_csv >> upload_log_to_sftp >> send_import_complete_email

        return send_logs_start, send_import_complete_email

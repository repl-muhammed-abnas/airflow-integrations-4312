import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False) as send_logs:
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ get_master_log() | load_all_records() | length > 0 }}',
            yes_task=['get_logged_errors',
                      'get_logged_success'],
            no_task='finish',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
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
            # pylint: disable=line-too-long
            header=['username','Status','Jobid','Details','Timesheet period','Employee Id','Error Message'],
            row=[
                '{{ item.properties.username}}',
                '{{ item.properties.status}}',
                '{{ item.ecid }}',
                '{{ item.properties.details}}',
                '{{ item.properties.timesheetperiod}}',
                '{{ item.properties.employeeid}}',
                '{{ item.properties.errormessage}}'
            ],
            )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/{{ dag_run_ecid() | replace(":", "-") }}' + '_Timesheetsubmitlogs' +
            '{{current_time("%m%d%YT%H%M%S")}}' + '_Disabled_Users.csv',
        )

        send_mail_message = rail.EmailOperator(
            task_id='send_mail_message',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Force Approve Timesheets For Disabled Users - {{ current_time_in_specified_tz("America/New_York") }}''',
            html_content='templates/emails/complete_mail.html',
        )

        has_any_entries_in_log >> rail.Label("Yes") >> [
            get_logged_errors, get_logged_success] >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp >> send_mail_message
        has_any_entries_in_log >> rail.Label("No") >> finish

        return send_logs

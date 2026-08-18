import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('create_allowance_log') }}",
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
            remote_filepath=config.user_allowance_log_path +
            '{{ dag_run_ecid() | replace(":", "-")}}' +
            "{{dag_run.conf.log_file_name_postfix}}",
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{get_company_key()+" | Australia - User allowance import job has completed - "+current_time_in_specified_tz("Australia/Sydney","%m-%d-%Y")}}',
            html_content="templates/email/email_allowance_import_completed.html",
        )

        render_logs_csv >> upload_log_to_sftp >> send_import_complete_email

        return render_logs_csv, send_import_complete_email

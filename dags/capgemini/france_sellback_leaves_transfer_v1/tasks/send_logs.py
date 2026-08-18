from datetime import timedelta
import pendulum
from capgemini.france_sellback_leaves_transfer_v1.utils.request_payload import do_format_logs
import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('format_logs') | to_json }}",
            header=['Username', 'Employee ID', 'Sell Back Source Time Off Type', 'Sell Back Amount',
                'Sell Back To Time Off Type', 'Status', 'Comments', 'RunID'],
            row=['{{ item.username }}',
                 '{{ item.employee_id }}',
                 '{{ item.sellback_source_timeoff_type }}',
                 '{{ item.sellback_amount }}',
                 '{{ item.sellback_dest_timeoff_type }}',
                 '{{ item.status }}',
                 '{{ item.comments }}',
                 '{{ item.jobid }}']
        )

        get_log_filename = rail.PythonOperator(
            task_id='get_log_filename',
            python_callable=lambda: config.log_file_prefix + '_France_Sellback_Leaves_Transfer_' +
                pendulum.now(config.time_zone).strftime("%Y%m%d_%H%M%S") + '.csv'
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/{{ result("get_log_filename") }}',
        )

        send_complete_email = rail.EmailOperator(
            task_id='send_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Sell Back Leaves Transfer for France is " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                       completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz("' + config.time_zone +'") }}',
            html_content="/templates/emails/success.html",
            params={
                'log_filepath': config.log_filepath,
                'time_zone': config.time_zone
            }
        )

        format_logs >> render_logs_csv
        render_logs_csv >> get_log_filename >> upload_log_to_sftp >> send_complete_email

        return format_logs, send_complete_email

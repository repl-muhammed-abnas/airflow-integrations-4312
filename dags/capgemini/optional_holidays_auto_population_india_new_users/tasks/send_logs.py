import pendulum
from airflow.models import Variable
import rail


def get_send_logs(config):
    with rail.TaskGroup(group_id='send_logs', prefix_group_id=False):
        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ result("create_log") | load_all_records() | length > 0 }}',
            yes_task='get_logged_success',
            no_task='fail_with_empty_log',
        )

        fail_with_empty_log = rail.FailOperator(
            task_id='fail_with_empty_log',
            message='No entries in log',
        )

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            log='{{ result("create_log") }}',
            severity='Success',
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            log='{{ result("create_log") }}',
            severity='Error',
        )

        get_logged_skipped = rail.FilterLogEntriesOperator(
            task_id='get_logged_skipped',
            log='{{ result("create_log") }}',
            severity='Skipped',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('create_log') }}",
            header=['State', 'Username', 'Employee ID',
                    'Booking Date', 'Status', 'Comments', 'ECID'],
            row=['{{ item.properties | attr_or_default("state", "") }}', '{{  item.properties | attr_or_default("username", "") }}',
                 '{{ item.properties | attr_or_default("employee_id", "") }}', '{{ item.properties | attr_or_default("booking_date", "") }}',
                 '{{ item.properties | attr_or_default("status", "") }}', '{{ item.message }}',
                 '{{ item.ecid }} | {{ item.properties | attr_or_default("dag_run_id", "") }}']
        )

        get_log_filename = rail.PythonOperator(
            task_id='get_log_filename',
            python_callable=lambda: config.log_file_prefix + '_Optional_Holiday_Booking_New_Users_Logs_' +
                pendulum.now(config.time_zone).strftime("%Y%m%d_%H%M%S") + '.csv'
        )

        upload_logs_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_logs_to_s3',
            source='{{ result("render_logs_csv") }}',
            key_name=config.s3_log_filepath +
            '/{{ result("get_log_filename") }}',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/{{ result("get_log_filename") }}',
        )

        subject = '{{ get_company_key() + " | The Auto population of Optional holiday booking for New Users is " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + result("logging_details").process_start_time }}'

        send_complete_email = rail.EmailOperator(
            task_id='send_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject=subject,
            html_content="/templates/emails/new_users_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        finish_log_generation = rail.EmptyOperator(
            task_id='finish_log_generation'
        )

        has_any_entries_in_log >> rail.Label(
            "Yes") >> get_logged_success >> get_logged_errors >> get_logged_skipped >> render_logs_csv
        render_logs_csv >> get_log_filename >> upload_logs_to_s3 >> upload_log_to_sftp >> send_complete_email >> finish_log_generation
        has_any_entries_in_log >> rail.Label(
            "No") >> fail_with_empty_log >> finish_log_generation

        return has_any_entries_in_log, finish_log_generation

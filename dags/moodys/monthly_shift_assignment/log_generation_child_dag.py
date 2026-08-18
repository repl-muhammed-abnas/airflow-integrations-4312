from datetime import timedelta
import rail
from moodys.monthly_shift_assignment.utils import python_callable_method


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/moodys/monthly_shift_assignment/config.py


def create_log_monthly_shift_update_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'moodysemea_monthlyshiftassignment_loggeneration_child_{config.instance}',
        description=f'Moodysemea_Monthly shift assignment_log generation V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable_method.do_format_logs
        )

        create_monthly_shift_log_csv = rail.WriteCSVFileOperator(
            task_id='create_monthly_shift_log_csv',
            source=lambda: rail.result('format_logs'),
            header=['parentjobid',
                    'childjobid',
                    'loginname',
                    'shiftname',
                    'status',
                    'details'],
            row=[
                '{{ item.parentjobid}}',
                '{{ item.childjobid }}',
                '{{ item.loginname }}',
                "{{ item.shiftname}}",
                "{{ item.status}}",
                '{{ item.details }}']
        )

        def file_upload_failed(context):
            # pylint: disable=line-too-long
            subject = '{{ get_company_key() }} | Monthly Shift Assignment - Uploading Logs to SFTP failed {{ current_time_in_specified_tz("America/Los_Angeles")'
            email = rail.EmailOperator(
                task_id='send_sftp_failure_payload_email',
                to=config.tenant_email,
                bcc=config.internal_logs_email,
                subject=subject,
                html_content='templates/email/sftp_upload_failure.html',
                files=['{{ dag_run.conf.log_filename }}']
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content='{{ result("create_monthly_shift_log_csv") }}',
            remote_filepath=config.log_filepath +
            '/{{ dag_run.conf.log_filename }}',
            on_failure_callback=file_upload_failed
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("create_monthly_shift_log_csv")}}',
            output_file_name='{{ dag_run.conf.log_filename }}',
            expires_in_seconds=7 * 24 * 60 * 60,
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Error', rail.result('format_logs')))), 'length')
        )

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Exception', rail.result('format_logs')))), 'length')
        )

        get_success_logs = rail.PythonOperator(
            task_id='get_success_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Success', rail.result('format_logs')))), 'length')
        )

        get_skipped_logs = rail.PythonOperator(
            task_id='get_skipped_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Skipped', rail.result('format_logs')))), 'length')
        )

        send_monthly_shift_update_email = rail.EmailOperator(
            task_id='send_monthly_shift_update_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Monthly Shift Assignment - "  }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time_in_specified_tz("America/Los_Angeles") }}',
            html_content="templates/email/monthly_shift_assignment.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'input_file_size': '{{ dag_run.conf.child_log | length }}',
                'processed': 'Yes',
                'error_message': '',
                'success': '{{ result("get_success_logs", key="length") }}',
                'exception': '{{ result("get_exception_logs", key="length") }}',
                'error': '{{ result("get_errored_logs", key="length")}}'
            }
        )

        format_logs >> create_monthly_shift_log_csv >> upload_log_to_sftp >> generate_download_link \
            >> [get_errored_logs, get_exception_logs, get_skipped_logs, get_success_logs] >> \
            send_monthly_shift_update_email >> log_to_sumo

        return dag


rail.for_each_instance(create_log_monthly_shift_update_child_dag)

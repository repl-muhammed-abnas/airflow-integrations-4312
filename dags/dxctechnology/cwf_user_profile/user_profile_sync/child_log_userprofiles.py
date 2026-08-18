from datetime import timedelta
import rail
from dxctechnology.cwf_user_profile.user_profile_sync.utils.python_callable_method import do_format_logs


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/cwf_user_profile/user_profile_sync/config.py


def create_log_userprofile_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_cwf_userprofiles_log_child_{config.instance}',
        description=f'DXC_Fieldglass CWFUserProfiles_Child_Log Generation {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_user_profile_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'Hpid',
                'Action',
                'Status',
                'Details',
                'Job ID'
            ],
            row=[
                '{{ item.userid }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.jobid }}'
            ]
        )

        def file_upload_failed(context):
            # pylint: disable=line-too-long
            subject = "{{ get_company_key() }} | Replicon user sync for CWF worker profile - Uploading Logs to SFTP failed - {{ current_time_in_specified_tz() }}"
            email = rail.EmailOperator(
                task_id='send_time_data_to_sftp_failure_email',
                to=config.alert_email,
                subject=subject,
                html_content='templates/emails/email_sftp_upload_failed.html',
                params={
                    'dag_id': f'dxctechnology_cwf_userprofiles_master_{config.instance}'
                },
                files=['{{ dag_run.conf.log_filename }}']
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/{{ dag_run.conf.log_filename }}',
            on_failure_callback=file_upload_failed
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

        get_add_user_logs = rail.PythonOperator(
            task_id='get_add_user_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['action'] == 'Add', rail.result(
                    'format_logs')))), 'length')
        )

        get_update_user_logs = rail.PythonOperator(
            task_id='get_update_user_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['action'] == 'Update', rail.result(
                    'format_logs')))), 'length')
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon user sync for CWF worker profile - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time_in_specified_tz() }}',
            html_content='templates/emails/email_import_complete.html',
            params={
                'log_filepath': config.log_filepath
            }
        )

        format_logs >> render_logs_csv >> upload_log_to_sftp >> \
            [get_errored_logs, get_exception_logs,
             get_skipped_logs, get_success_logs, get_add_user_logs, get_update_user_logs] >> \
            send_import_complete_email

        return dag


rail.for_each_instance(create_log_userprofile_child_dag)

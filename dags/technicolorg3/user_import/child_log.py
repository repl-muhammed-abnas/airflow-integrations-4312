from datetime import timedelta
import rail
from technicolorg3.user_import.utils.python_callable_method import do_format_logs

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/user_import/config.py


def create_log_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_user_import_child_log_{config.instance}',
        description=f'Technicolor Child log {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
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
                'User Name',
                'Global ID',
                'Action',
                'Status',
                'Details',
                'New Location',
                'Location',
                'Job ID'],
            row=[
                '{{ item.username }}',
                '{{ item.globalid }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.new_location }}',
                '{{ item.location }}',
                '{{ item.jobid }}'
            ]
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/Logs_{{ dag_run.conf.time }}_{{ dag_run.conf.filename }}.csv'
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

        get_new_locations = rail.PythonOperator(
            task_id='get_new_locations',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['new_location'] == 'Yes', rail.result('format_logs')))), 'length')
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | User import - " }} \
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
            html_content='templates/email/import_complete.html',
            params={
                'log_filepath': config.log_filepath
            }
        )

        upload_failure_email = rail.EmailOperator(
            task_id='upload_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject="{{ get_company_key() }} | User Import - Uploading Logs to SFTP failed - {{ current_time_in_specified_tz() }}",
            html_content='templates/email/sftp_upload_failed.html',
            params={
                'dag_id': f'technicolorg3_user_import_master_{config.instance}'
            }
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message='{{ get_error_message() }}'
        )

        format_logs >> render_logs_csv >> upload_log_to_sftp >> get_errored_logs >> get_exception_logs >> get_new_locations >> \
            send_import_complete_email

        upload_log_to_sftp >> rail.Label(
            'On Error') >> upload_failure_email >> fail_dag

        return dag


rail.for_each_instance(create_log_child_dag)

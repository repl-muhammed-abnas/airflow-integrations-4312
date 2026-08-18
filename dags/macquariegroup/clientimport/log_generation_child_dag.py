from datetime import timedelta
import rail
from macquariegroup.clientimport.utils import python_callable_method


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/macquariegroup/clientimport/config.py


def create_client_import_log_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'macquarie_client_import_loggeneration_{config.instance}',
        description=f'Macquarie Client Import Loggeneration_V1.0 {config.instance}',
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

        create_client_import_log_csv = rail.WriteCSVFileOperator(
            task_id='create_client_import_log_csv',
            source=lambda: rail.result('format_logs'),
            header=['Date',
                    'Client',
                    'Code',
                    'Location',
                    'Group',
                    'Division',
                    'Location Name',
                    'Business Unit Name',
                    'Status',
                    'Details',
                    'JobID'],
            row=[
                '{{ current_time_in_specified_tz("Australia/Sydney","%m/%d/%Y") }}',
                '{{ item.client }}',
                '{{ item.code }}',
                "{{ item.location }}",
                "{{ item.group }}",
                "{{ item.division }}",
                "{{ item.locationname }}",
                "{{ item.businessunit }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.childjobid }}"]
        )

        def file_upload_failed(context):
            # pylint: disable=line-too-long
            subject = '{{ get_company_key() }} | Client Import - Failure uploading Logs to SFTP  - {{ dag_run.conf.time }}'
            email = rail.EmailOperator(
                task_id='send_sftp_failure_payload_email',
                to=config.tenant_email,
                bcc=config.internal_logs_email,
                subject=subject,
                html_content='templates/email/sftp_upload_failure.html',
                files=[
                    '{{ dag_run.conf.parentjobid | replace(":", "-") }}_' + config.client_import_log_file]
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content='{{ result("create_client_import_log_csv") }}',
            remote_filepath=config.log_filepath +
            '/{{ dag_run.conf.parentjobid | replace(":", "-") }}_' +
            config.client_import_log_file,
            on_failure_callback=file_upload_failed
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("create_client_import_log_csv")}}',
            output_file_name='{{ dag_run.conf.parentjobid | replace(":", "-") }}_' +
            config.client_import_log_file,
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

        get_clients_added_count = rail.PythonOperator(
            task_id='get_clients_added_count',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['details'] == 'Client Added', rail.result('format_logs')))), 'length')
        )

        get_clients_updated_count = rail.PythonOperator(
            task_id='get_clients_updated_count',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['details'] == 'Client Updated', rail.result('format_logs')))), 'length')
        )

        get_clients_disabled_count = rail.PythonOperator(
            task_id='get_clients_disabled_count',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['details'] == 'Client Disabled', rail.result('format_logs')))), 'length')
        )

        send_client_import_email = rail.EmailOperator(
            task_id='send_client_import_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Client Import - "  }} \
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
            html_content="templates/email/client_import.html"
        )

        format_logs >> create_client_import_log_csv >> upload_log_to_sftp >> generate_download_link \
            >> [get_errored_logs, get_exception_logs, get_skipped_logs, get_success_logs, get_clients_added_count,
                get_clients_updated_count, get_clients_disabled_count] >> send_client_import_email

        return dag


rail.for_each_instance(create_client_import_log_child_dag)

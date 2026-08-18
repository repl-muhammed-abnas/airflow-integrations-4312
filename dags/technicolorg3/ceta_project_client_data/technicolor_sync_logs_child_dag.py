from datetime import datetime
import pendulum
import rail


null = None

# pylint: disable=too-many-statements


def create_sync_logs_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_project_client_details_sync_logs_{config.instance}',
        description=f'Technicolor CETA_Client_Project_Logs {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 10, 10),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        header = ['Mill_MPC', 'Client', 'Project',
                  'Status', 'Action', 'Details', 'Reference']

        get_client_project_log = rail.CreateLogOperator(
            task_id='get_client_project_log',
            tenant_wide_name=f'{config.client_project_logs}',
            existing_log_mode='append',
        )

        def do_filter_log(log):
            current_time = pendulum.now()
            timestamp = datetime.strptime(
                log['timestamp'], '%Y-%m-%dT%H:%M:%S.%f%z')
            return timestamp <= current_time

        filter_log = rail.FilterLogEntriesOperator(
            task_id='filter_log',
            log="{{ result('get_client_project_log')}}",
            filter_callable=do_filter_log,
            remove_filtered_entries=True,
        )

        has_any_data = rail.HasDataOperator(
            task_id='has_any_data',
            source='{{ result("filter_log") }}',
            yes_task=['get_errored_logs',
                      'get_exception_logs', 'get_success_logs'],
            no_task='delete_this_dagrun'
        )

        get_errored_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_logs',
            log="{{ result('filter_log') }}",
            properties={'status': 'Error'}
        )

        get_exception_logs = rail.FilterLogEntriesOperator(
            task_id='get_exception_logs',
            log="{{ result('filter_log') }}",
            properties={'status': 'Exception'}
        )

        get_success_logs = rail.FilterLogEntriesOperator(
            task_id='get_success_logs',
            log="{{ result('filter_log') }}",
            properties={'status': 'Success'}
        )

        create_csv_file = rail.WriteCSVFileOperator(
            task_id="create_csv_file",
            source=lambda: rail.result('filter_log'),
            header=header,
            row=['{{ item.properties.db }}', '{{ item.properties.client }}', '{{ item.properties.project }}', '{{ item.properties.status }}',
                 '{{ item.properties.action }}', '{{ item.properties.details }}', '{{ item.properties.reference }}']
        )

        def file_upload_failed(context):
            subject = '{{ get_company_key() }} | CETA Client Project Sync - Failed while uploading logs to SFTP - {{ current_time("%Y%m%dT%H%M%S") }}'
            email = rail.EmailOperator(
                task_id='send_sftp_failure_payload_email',
                to=config.tenant_email,
                bcc=config.internal_logs_email,
                subject=subject,
                html_content='templates/email/sftp_failure.html',
                files=[('{{ result("create_csv_file") }}')]
            )
            email.render_template_fields(context)
            email.execute(context)

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("create_csv_file")}}',
            output_file_name='{{current_time("%Y%m%dT%H%M%S")}}_CETA_Client_Project_sync_logs.csv',
            expires_in_seconds=7 * 24 * 60 * 60,
            on_failure_callback=file_upload_failed
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | CETA Client Project Sync - "  }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="templates/email/import_complete.html"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_client_project_log >> filter_log >> has_any_data
        has_any_data >> rail.Label(
            'Yes') >> [get_errored_logs, get_exception_logs, get_success_logs] \
            >> create_csv_file >> generate_download_link >> send_import_complete_email
        has_any_data >> rail.Label('No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_sync_logs_child_dag)

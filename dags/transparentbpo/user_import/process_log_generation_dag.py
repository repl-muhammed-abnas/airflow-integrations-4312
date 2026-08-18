"""
TransparentBPO Create Supervisor Child DAG
Creates a new supervisor user in Replicon
"""
from datetime import timedelta
import rail
from transparentbpo.user_import.utils import custom_methods

null = None


def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_dag_id,
        description=f'TransparentBPO Process Log Generation Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_log_generation,
    ) as dag:

        # View DAG configuration
        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'employeenumber',
                'username',
                'status',
                'details',
                'jobid'
            ],
            row=[
                '{{ item.employeenumber }}',
                '{{ item.user_name }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.jobid }}'
            ],

            footer=[
                'Number of records found:{{ result("format_logs", key="total_record_count")}}',
                'Number of records processed:' +
                '{{- result("format_logs", key="success_record_count") + result("format_logs",key="error_record_count")}}',
                'Number of success records: {{ result("format_logs", key="success_record_count")}}',
                'Number of error records: {{ result("format_logs", key="error_record_count") }}'
            ]
        )

        get_email_and_log_file_details = rail.PythonOperator(
            task_id="get_email_and_log_file_details",
            python_callable=lambda dag_run: custom_methods.get_email_details_callable(
                dag_run, config.time_zone)
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name="{{result('get_email_and_log_file_details').log_file_name}}",
            expires_in_seconds=7*24*60*60,
        )

        # upload_log_to_sftp = rail.SFTPUploadFileOperator(
        #     task_id='upload_log_to_sftp',
        #     content="{{ result('render_logs_csv') }}",
        #     remote_filepath=config.log_filepath +
        #     "/{{result('get_email_and_log_file_details').log_file_name}}"
        # )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Bamboohr user sync to Replicon " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with failed records  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="templates/emails/import_complete_mail.html"
        )

        format_logs >> render_logs_csv >> get_email_and_log_file_details >> generate_download_link >> send_import_complete_email

    return dag


rail.for_each_instance(create_child_dag)

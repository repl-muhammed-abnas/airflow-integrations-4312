from datetime import timedelta
from fileinput import filename
from airflow.models import Variable
import rail
from sweethometherapyllc.time_entry_import.utils import custom_methods

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation,
        description=f'sweethometherapyllc Time Import Child - Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_log_gen_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='format_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='format_logs',
            end_task='batch_end',
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda dag_run:custom_methods.get_record_count_by_status(
                dag_run),
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda dag_run: dag_run.conf['log'],
            header=[
                'Entry KeyID',
                'Therapist',
                'Date of Service',
                'Service Name',
                "Hours",
                "Action",
                "Status",
                "Details",
                "ECID | Run ID",
            ],
            row=[
                "{{ item.properties.entry_keyid | default('') }}",
                "{{ item.properties.therapist | default('') }}",
                "{{ item.properties.date_of_service | default('') }}",
                "{{ item.properties.service_name | default('') }}",
                "{{ item.properties.hours | default('') }}",
                "{{ item.properties.action | default('') }}",
                "{{ item.properties.status | default('') }}",
                "{{ item.properties.details | default('') }}",
                "{{item.ecid}}",
            ]
        )

        get_email_details = rail.PythonOperator(
            task_id='get_email_details',
            python_callable=lambda dag_run: custom_methods.get_email_details(
                config.timezone,
                config.log_filepath,
                dag_run
            )
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ result("get_email_details")["log_file_name"] }}',
            expires_in_seconds=7*24*60*60,
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath= config.log_filepath +'/{{ result("get_email_details")["log_file_name"] }}',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                "+config.internal_logs_email+"\
            {%- else -%}\
                "+config.alert_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() + " | Time Import is " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/success_email.html"
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
        can_run_batch_task >> rail.Label("No") >> format_logs >> render_logs_csv >> get_email_details >> generate_download_link \
            >> upload_log_to_sftp >> send_import_complete_email >> batch_end

    return dag

rail.for_each_instance(create_child_dag)

from datetime import timedelta
from airflow.models import Variable
import rail
from ipipeline.user_import_v2.utils import custom_methods


def create_log_generation_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_child_dag_id,
        description=f'iPipeline User Import Child - Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_log_generation_child_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='render_logs_csv'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='render_logs_csv',
            end_task='batch_end',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda dag_run: rail.load_all_records(
                dag_run.conf["userlogs"]),
            header=["recordUniqId", "action",
                    "status", "processInfo", "jobID"],
            row=lambda item: [
                item["employeeid"],
                item["action"],
                item["status"],
                item["details"],
                item["runid"]
            ],
        )

        get_email_log_details = rail.PythonOperator(
            task_id='get_email_log_details',
            python_callable=lambda dag_run: custom_methods.get_email_log_details(
                config.log_filepath, dag_run, config.time_zone, config.STANDARD_EMAIL_DATE_FORMAT)
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ dag_run.conf.log_filename }}',
            expires_in_seconds=config.log_expiry,
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            "{{ dag_run.conf.log_filename }}",
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if dag_run.conf.error_count == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon User Import - "}} \
                {%- if dag_run.conf.error_count > 0 -%} \
                    Completed with errors \
                {%- else -%} \
                    {%- if dag_run.conf.exception_count > 0 -%} \
                        Completed with exceptions \
                    {%- else -%} \
                        Completed successfully \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " | " + current_time_in_specified_tz() }}',
            html_content="templates/emails/import_complete.html",
            params={
                "log_file_path": config.log_filepath
            }
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
        can_run_batch_task >> rail.Label("No") >> render_logs_csv
        render_logs_csv >> get_email_log_details >> generate_download_link >> upload_log_to_sftp >> send_import_complete_email >> batch_end

    return dag


rail.for_each_instance(create_log_generation_child_dag)

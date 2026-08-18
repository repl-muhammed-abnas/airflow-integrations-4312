from datetime import timedelta
from airflow.models import Variable
import rail
from wcg.user_import.utils.custom_methods import do_format_logs

null = None


def create_log_generation_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_child_dag_id,
        description="WCG User Import - Generate Logs and Send Emails",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_log_generation_child_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'Employee ID',
                'First Name',
                'Last Name',
                'Action',
                'Status',
                'Details',
                'Jobid',
            ],
            row=[   '{{ item.employeeid }}',
                    '{{ item.firstname }}',
                    '{{ item.lastname }}',
                    '{{ item.action }}',
                    '{{ item.status }}',
                    '{{ item.details }}', 
                    '{{ item.jobid }}']
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.sftp_log_path +
            '/'+"{{dag_run.conf.log_filename}}",
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon User Import " }} \
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
            html_content="templates/emails/import_complete.html",
            params={
                'log_filepath': config.sftp_log_path,
            }
        )

        format_logs >> render_logs_csv >> upload_log_to_sftp >> send_import_complete_email

    return dag


rail.for_each_instance(create_log_generation_child_dag)

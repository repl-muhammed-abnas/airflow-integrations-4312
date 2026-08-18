from datetime import timedelta
import rail

from cohnreznick.timeentry_sync.utils.custom_methods import do_format_logs

# pylint: disable=too-many-statements


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation,
        description=' Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_log_generation,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=do_format_logs,
            show_return_value_in_logs=False,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.load_json_artifact(rail.result("format_logs")),
            header=[
                'Entry Id',
                'Employee Id',
                'Status',
                'Action',
                'Details',
                'Job ID'],
            row=[
                '{{item.properties.entry_id}}',
                '{{ item.properties.employee_id }}',
                '{{ item.properties.status }}',
                '{{ item.properties.action }}',
                '{{ item.properties.details }}',
                '{{ item.ecid }}'],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/'+"{{dag_run.conf.log_filename}}"
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='get_errored_logs') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='''{{ get_company_key() }} |  Replicon Time Entry Sync - \
                {%- if result("format_logs", key="get_errored_logs") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="get_exception_logs") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time() }}''',
            html_content="templates/emails/email_import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        format_logs >> render_logs_csv >> upload_log_to_sftp >> send_import_complete_email
    return dag


rail.for_each_instance(create_child_dag_wbs)

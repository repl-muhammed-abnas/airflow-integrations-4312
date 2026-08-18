from datetime import timedelta
import rail

from dxctechnology.gsap_task_import_project_fields_v2.utils.python_callable_method import do_format_wbs_logs

# pylint: disable=too-many-statements
def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.log_generation,
        description='DXC_GSAP_TAsK_Automation Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_log_generation,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        format_task_logs = rail.PythonOperator(
            task_id = "format_task_logs",
            python_callable=do_format_wbs_logs,
            show_return_value_in_logs=False,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )


        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda : rail.result("format_task_logs"),
            header=[
                'Level',
                'WBS',
                'Task name',
                'Status',
                'Details',
                'Job ID'],
            row=[
                '{{item.properties.Level}}',
                '{{ item.properties.wbs }}',
                '{{ item.properties.task_name }}',
                '{{ item.properties.status }}',
                '{{ item.message }}',
                '{{ item.ecid }}'],
            footer=[
                'Number of Records Processed Successfully: {{result("format_task_logs", key="get_successful_attribute_logs")}}',
                'Number of Records with Error: {{result("format_task_logs", key="get_errored_attribute_logs")}}',
                'Number of Records with Exception: {{result("format_task_logs", key="get_exception_attribute_logs")}}',
                '',
                ''],
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
            bcc="{%- if result('format_task_logs', key='get_errored_attribute_logs') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='''{{ get_company_key() }} |  Replicon project field sync for GSAP Task - \
                {%- if result("format_task_logs", key="get_errored_attribute_logs") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_task_logs", key="get_errored_attribute_logs") > 0 -%} \
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

        format_task_logs >> render_logs_csv >>upload_log_to_sftp >> send_import_complete_email
    return dag


rail.for_each_instance(create_child_dag_wbs)

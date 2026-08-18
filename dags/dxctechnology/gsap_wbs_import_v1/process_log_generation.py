from datetime import timedelta
import rail

from dxctechnology.gsap_wbs_import_v1.utils import python_callable_methods

# pylint: disable=too-many-statements
def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_wbs_import_child_process_log_generation_{config.instance}_v1',
        description='DXC_GSAP_WBS_Automation Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_log_generation,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.child_wait_execution_timeout_days),
            python_callable=python_callable_methods.do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            # pylint: disable=line-too-long
            header=['{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                    'Number of Rows:' + '{{- result("format_logs", key="error_record_count") + result("format_logs", key="success_record_count") +\
                      result("format_logs", key="exception_record_count") + result("format_logs", key="skipped_record_count")}}',
                    'Function: GSAP WBS Master inbound', '', '', ''],
            row=['{{ item | attr_or_default("projectname", "") }}',
                 '{{ item | attr_or_default("status", "") }}', '{{ item.message }}', '{{ item.jobid }}'],
            footer=['Number of Records Errored: {{ result("format_logs", "error_record_count") }}',
                    'Number of Records Processed Successfully: {{ result("format_logs", "success_record_count") }}',
                    'Number of Records with Exception: {{ result("format_logs", "exception_record_count") }}',
                    'Number of Records Skipped: {{ result("format_logs", "skipped_record_count") }}',
                    '', '', ''],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/'+"{{dag_run.conf.log_filename}}",
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon project sync for GSAP WBS  - " }} \
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
                'log_filepath': config.log_filepath,
            }
        )

        format_logs >> render_logs_csv >> upload_log_to_sftp >> send_import_complete_email

    return dag


rail.for_each_instance(create_child_dag_wbs)

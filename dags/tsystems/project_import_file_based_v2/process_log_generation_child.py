from datetime import timedelta
from os import path
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split
import rail
from tsystems.project_import_file_based_v2.utils import custom_methods

def create_process_log_generation_dag(config):
    """Child DAG to process log generation and email notifications"""
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_dag_id,
        description='T-Systems Process Log Generation Child DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_second_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.format_integration_logs
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'projectid',
                'projectname', 
                'clientcode',
                'status',
                'action',
                'details',
                'ecid'
            ],
            row=[
                '{{ item.properties.projectid }}',
                '{{ item.properties.projectname }}',
                '{{ item.properties.clientcode }}',
                '{{ item.properties.status }}',
                '{{ item.properties.action }}',
                '{{ item.properties.details }}',
                '{{ item.ecid }}'
            ],
        )

        def fetch_log_file_name(dag_run):
            ecid = get_dagrun_ecid(dag_run).replace(":", "-")
            file_name = path.split(dag_run.conf["log_file_name"])[1]
            base_name = split(string=file_name, separator=".")[0]
            return f'{rail.get_company_key()}_log_{ecid}_{base_name}'

        get_log_file_name = rail.PythonOperator(
            task_id='get_log_file_name',
            python_callable=fetch_log_file_name
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath + '/{{ result("get_log_file_name") }}.csv',
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name="{{ result('get_log_file_name') }}.csv",
            expires_in_seconds=7*24*60*60 
        )

        send_completion_email = rail.EmailOperator(
            task_id='send_completion_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Project import - " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y/%m/%d/%H:%M:%S") }}',
            html_content="templates/emails/import_complete.html",
            params={
                'log_filepath': config.log_filepath
            }
        )

        format_logs >> render_logs_csv >> get_log_file_name >> upload_logs_to_sftp

        upload_logs_to_sftp >> generate_downloadable_link >> send_completion_email

    return dag

rail.for_each_instance(create_process_log_generation_dag)
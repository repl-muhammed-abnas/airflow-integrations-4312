from datetime import timedelta
import rail

from eisner_amper.project_import_internal_bulk_processing.utils import python_callable_methods

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation,
        description='Eisner Amper Project Data Import - internal Records Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_log_generation,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable_methods.do_format_logs
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'Client Code',
                'Project Code',
                'Task Name',
                'Task Code',
                'Action',
                'Status',
                'Details',
                'Jobid'],
            row=[
                '{{ item.clientcode }}',
                '{{ item.projectcode }}',
                '{{ item.taskname }}',
                '{{ item.taskcode }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.jobid }}'],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content='{{ result("render_logs_csv") }}',
            remote_filepath=config.log_filepath +
            '/{{ dag_run.conf.log_file_name }}'
        )

        send_complete_email = rail.EmailOperator(
            task_id='send_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon EisnerAmper Project Import (BULK Processing) for internal sync -  " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/emails/complete_email.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        format_logs >> render_logs_csv >> upload_log_to_sftp >> send_complete_email

    return dag

rail.for_each_instance(create_child_dag_wbs)

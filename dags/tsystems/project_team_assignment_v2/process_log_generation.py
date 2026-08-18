from datetime import timedelta
import pendulum
import rail
from tsystems.project_team_assignment_v2.utils.python_callable import do_format_logs

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation_dagid,
        description='Tsystems Project Team Allocation - Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_log_generation,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs,
            show_return_value_in_logs=False
        )

        if_logs_present = rail.IfOperator(
            task_id="if_logs_present",
            test=lambda: len(rail.result('format_logs')) > 0,
            yes_task="render_logs_csv",
            no_task="finish_task"
        )

        render_logs_csv = rail.WriteCSVFileOperator2(
            task_id='render_logs_csv',
            source=lambda **kwargs: rail.result('format_logs'),
            execution_timeout=timedelta(hours=config.execution_timeout_write_csv),
            thread_pool_size=config.thread_pool_size_write_csv,
            header=[
                'Assignment ID',
                'Project ID',
                'Individual ID',
                'Cost Object ID',
                'Start Date',
                'End Date',
                'Hours',
                'Status',
                'Details',
                'Jobid'
            ],
            row=[   '{{ item.assignment_id }}',
                    '{{ item.decidalo_project_id }}',
                    '{{ item.individual_id }}',
                    '{{ item.cost_object_id }}',
                    '{{ item.search_period_start }}',
                    '{{ item.search_period_end }}',
                    '{{ item.hours }}',
                    '{{ item.status }}',
                    '{{ item.details }}',
                    '{{ item.jobid }}']
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/'+"{{ dag_run.conf.log_filename }}",
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('render_logs_csv')}}",
            output_file_name="{{dag_run.conf.log_filename}}",
            expires_in_seconds=7*24*60*60
        )

        process_end_time = rail.PythonOperator(
            task_id='process_end_time',
            python_callable=lambda: pendulum.now(config.time_zone).strftime(config.DATETIMEFORMAT)
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Project Team Assignment " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="templates/email/import_complete.html",
            params={
                'log_filepath': config.log_filepath
            }
        )

        finish_task = rail.EmptyOperator(
            task_id='finish_task'
        )

        format_logs >> if_logs_present
        
        if_logs_present >> rail.Label('Yes') >> render_logs_csv >> upload_log_to_sftp >> generate_downloadable_link >> process_end_time >> send_import_complete_email
        if_logs_present >> rail.Label('No') >> finish_task

    return dag

rail.for_each_instance(create_child_dag)

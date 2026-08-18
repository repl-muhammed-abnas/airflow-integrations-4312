from datetime import timedelta
from airflow.models import Variable
import rail

from galaxyusopcoinc.tiger_assignee_integration.utils import python_callable_method

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_tiger_assignee_integration_child_process_log_generation_{config.instance}',
        description='Vialto Partners Tiger Assignee Integration Process Log generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_log_generation,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='format_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days),
            start_task='format_logs',
            end_task='finish',
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.child_wait_execution_timeout_days),
            python_callable=python_callable_method.do_format_logs
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            execution_timeout=timedelta(days=config.child_wait_execution_timeout_days),
            header=['Project Name',
                    'Client Name',
                    'Client Shortname',
                    'Assignee ID',
                    'Assignee Status',
                    'Status',
                    'Details',
                    'JobID',
                    '{{ current_time("%d/%m/%YT%H:%M:%S") }}'],
            row=[
                '{{ item.projectname}}',
                '{{ item.clientname }}',
                '{{ item.clientshortname }}',
                "{{ item.assigneeid}}",
                "{{ item.assigneestatus}}",
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.jobid }}',]
        )

        get_logged_errors = rail.PythonOperator(
            task_id='get_logged_errors',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Error', rail.result('format_logs')))), 'length')
        )

        get_logged_exceptions = rail.PythonOperator(
            task_id='get_logged_exceptions',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Exception', rail.result('format_logs')))), 'length')
        )

        get_success_logs = rail.PythonOperator(
            task_id='get_success_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Success', rail.result('format_logs')))), 'length')
        )

        has_any_error_exception = rail.IfOperator(
            task_id="has_any_error_exception",
            test=lambda: rail.result('get_logged_errors','length') > 0 or
              rail.result('get_logged_exceptions','length') > 0,
            yes_task="upload_log_to_sftp",
            no_task='send_import_complete_email'
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath + "/{{dag_run.conf.log_filename}}",
        )


        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Tiger Assignee Integration - " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exceptions", key="length") > 0 -%} \
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

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> format_logs

        format_logs >> render_logs_csv >> get_logged_errors >> get_logged_exceptions >> get_success_logs >> has_any_error_exception
        has_any_error_exception >> rail.Label('Yes') >> upload_log_to_sftp >> send_import_complete_email
        has_any_error_exception >> rail.Label('No') >> send_import_complete_email >> finish

    return dag

rail.for_each_instance(create_child_dag_wbs)

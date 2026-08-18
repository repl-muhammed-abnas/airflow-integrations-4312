from datetime import timedelta
from airflow.models import Variable
import rail
from terraconconsultants.user_import.utils.python_callable_method import do_format_logs


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


def create_log_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_child_log_{config.instance}',
        description=f'Terraconconsultants Child log {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

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
                days=config.execution_timeout_days),
            start_task='format_logs',
            end_task='finish'
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs
        )

        is_logs_present = rail.IfOperator(
            task_id='is_logs_present',
            test="{{ result('format_logs') | length > 0 }}",
            yes_task='render_logs_csv',
            no_task='finish'
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'Login Name',
                'User URI',
                'Action',
                'Status',
                'Reason',
                'Job ID'],
            row=[
                '{{ item.loginname }}',
                '{{ item.uri }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.reason }}',
                '{{ item.jobid }}'
            ]
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/{{ dag_run.conf.filename }}_logs.csv'
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Error', rail.result('format_logs')))), 'length')
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | User import - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " " + current_time_in_specified_tz() }}',
            html_content='templates/email/import_complete.html',
            params={
                'log_filepath': config.log_filepath
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            'No') >> format_logs

        format_logs >> is_logs_present

        is_logs_present >> rail.Label(
            'Yes') >> render_logs_csv >> upload_log_to_sftp >> get_errored_logs >> \
            send_import_complete_email >> finish
        is_logs_present >> rail.Label(
            'No') >> finish

        return dag


rail.for_each_instance(create_log_dag)

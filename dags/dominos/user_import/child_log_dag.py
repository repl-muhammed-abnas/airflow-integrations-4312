from datetime import timedelta
from airflow.models import Variable
import rail
from dominos.user_import.utils.python_callable_method import do_format_logs


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/dominos/user_import/config.py


def create_log_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dominos_userimport_child_log_{config.instance}',
        description=f'Dominos user import Child log {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

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
            end_task='finish',
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
                'jobid',
                'username',
                'status',
                'failure/reason'
            ],
            row=[
                '{{ item.jobid }}',
                '{{ item.loginname }}',
                '{{ item.status }}',
                '{{ item.reason }}'
            ]
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
            subject='{{ get_company_key() + " | Replicon user import has been - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " " + dag_run.conf.jobstarttime }}',
            html_content='templates/email/import_complete.html',
            files=[
                ("Logs_userimport_{{ current_time('%m%d%Y') }}.csv", "{{ result('render_logs_csv') }}")]
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
            'Yes') >> render_logs_csv >> get_errored_logs >> send_import_complete_email >> finish
        is_logs_present >> rail.Label(
            'No') >> finish

        return dag


rail.for_each_instance(create_log_child_dag)

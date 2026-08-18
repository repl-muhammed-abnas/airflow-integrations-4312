from datetime import timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/oxfordfinancial/client_import_advisor/config.py


def create_child_process_log_pregeneration(config):
    with rail.create_airflow_dag(
        dag_id=f'oxfordfinancial_client_import_advisor_child_log_{config.instance}',
        description=f'OxfordFinancial Client Import Advisor Log Generation {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_log_generation_max_active_runs
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
            end_task='log_dagrun_to_sumo'
        )

        def do_format_logs():
            def load_records(log_artifact):
                try:
                    logs = rail.load_all_records(log_artifact)
                    return logs
                except:  # pylint: disable=bare-except
                    return []
            dag_run = rail.get_current_context()['dag_run']
            log_artifacts = dag_run.conf['logs']
            log_records = []
            if log_artifacts:
                for log in log_artifacts:
                    each_log_records = load_records(log)
                    if each_log_records:
                        log_records.extend(each_log_records)

            return list(map(lambda x: {
                **dict(x['properties'].items()),
                **{
                    'parentjobid': x['ecid'].split(':', 1)[0],
                    'childjobid': x['ecid'].split(':', 1)[1] if len(x['ecid'].split(':', 1)) > 1 else ''
                }}, log_records))
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            python_callable=do_format_logs
        )

        should_process_logs = rail.IfOperator(
            task_id="should_process_logs",
            test="{{ result('format_logs') | length > 0 }}",
            yes_task="render_logs_csv",
            no_task="log_dagrun_to_sumo"
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'Parent job ID',
                'Child job ID',
                'SF_18_Digit_ID',
                'Status',
                'Reason'
            ],
            row=[
                "{{ item.parentjobid }}",
                "{{ item.childjobid }}",
                "{{ item.sf18digitid }}",
                "{{ item.status }}",
                "{{ item.reason }}"
            ]
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Error', rail.result('format_logs')))), 'length')
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_email",
            to="{%- if result('get_errored_logs', key='length') ==0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Client Import - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    Completed with errors  \
                {%- else -%} \
                    Completed Successfully \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz("America/Los_Angeles") + " (" + dag_run.conf.filename + ") " }}',
            html_content="import_complete.html",
            files=[
                ('Clientlog_{{ dag_run.conf.filename }}', "{{ result('render_logs_csv') }}")]
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_dagrun_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> format_logs

        format_logs >> should_process_logs

        should_process_logs >> rail.Label(
            'Yes') >> render_logs_csv >> get_errored_logs >> send_import_complete_email >> log_dagrun_to_sumo

        should_process_logs >> rail.Label(
            'No') >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_child_process_log_pregeneration)

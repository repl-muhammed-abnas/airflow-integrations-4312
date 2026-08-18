from datetime import timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/balparag3/project_import/config.py


def create_child_process_log_pregeneration(config):
    with rail.create_airflow_dag(
        dag_id=f'balparag3_projectimport_child_log_{config.instance}',
        description=f'Balparag3 Project Log Pregeneration {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_log_generation_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
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
            log_artifacts = dag_run.conf['user_logs']
            log_records = []
            if log_artifacts:
                for log in log_artifacts:
                    each_log_records = load_records(log)
                    if each_log_records:
                        log_records.extend(each_log_records)

            return list(map(lambda x: {
                **{k: v for k, v in x['properties'].items() if k in ('project_code', 'project_name',
                                                                     'status', 'details')},
                **{
                    'jobid': x['ecid']
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
                'Project Code',
                'Project Name',
                'Status',
                'Details',
                'Jobid'
            ],
            row=[
                '{{ item.project_code }}',
                '{{ item.project_name }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.jobid }}'
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name="Logs_" + "{{ current_time('%d%m%YT%H%M%S') }}_" +
            "{{ dag_run.conf.filename }}.csv",
            expires_in_seconds=7*24*60*60
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Error', rail.result('format_logs')))), 'length')
        )

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Exception', rail.result('format_logs')))), 'length')
        )

        get_skipped_logs = rail.PythonOperator(
            task_id='get_skipped_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Skipped', rail.result('format_logs')))), 'length')
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_email",
            to='{{ dag_run.conf.tenant_email }}',
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                "+config.internal_logs_email+"\
            {%- else -%}\
                "+config.alert_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() + " | Project import - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions \
                    {%- elif result("get_skipped_logs", key="length") > 0 -%} \
                        completed with skipped records \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y/%m/%d/%H:%M:%S") }}',
            html_content="email_templates/import_complete.html"
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.dagrun_log_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_dagrun_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> format_logs

        format_logs >> should_process_logs

        should_process_logs >> rail.Label(
            'Yes') >> render_logs_csv >> generate_download_link >> get_errored_logs >> \
            get_exception_logs >> get_skipped_logs >> send_import_complete_email >> log_dagrun_to_sumo

        should_process_logs >> rail.Label(
            'No') >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_child_process_log_pregeneration)

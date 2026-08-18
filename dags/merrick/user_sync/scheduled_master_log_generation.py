from datetime import timedelta, timezone, datetime
import itertools
from airflow.models import Variable, DagRun
import rail


# pylint:disable = too-many-statements
def create_log_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.log_generation_dag_id,
        description=f'User_Sync_dynamicwait_Loggeneration_Scheduled {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.log_generation_dag_interval,
        max_active_tasks=config.dag_max_active_tasks,
        start_date=datetime(2022, 1, 1),
    ) as dag:

        def get_dagruns_to_process(lookup_log_timestamp_var, lookup_log_timestamp_hours, dag_id):
            current_time = datetime.now(timezone.utc)
            lookup_timestamp_value = Variable.get(
                lookup_log_timestamp_var, default_var=None)
            query_execution_start_date = datetime.fromisoformat(lookup_timestamp_value) if lookup_timestamp_value else (
                current_time - timedelta(hours=lookup_log_timestamp_hours))
            dag_runs = []
            execution_dates = []
            for run in DagRun.find(dag_id=dag_id, state='success', execution_start_date=query_execution_start_date):
                execution_dates.append(run.execution_date)
                dag_runs.append(run.id)
            if execution_dates:
                max_execution_date = max(execution_dates)
                Variable.set(lookup_log_timestamp_var,
                             (max_execution_date + timedelta(seconds=1)).isoformat())
            return dag_runs

        get_log_dagruns_to_process = rail.PythonOperator(
            task_id='get_log_dagruns_to_process',
            python_callable=get_dagruns_to_process,
            op_args=[config.lookup_log_timestamp_var,
                     config.lookup_log_timestamp_hours,
                     config.master_dag_id]
        )

        is_log_dagruns_present = rail.IfOperator(
            task_id='is_log_dagruns_present',
            test="{{ result('get_log_dagruns_to_process') | length > 0 }}",
            yes_task='get_user_logs',
            no_task='delete_this_dagrun'
        )

        get_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_user_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='load_master_log',
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: list(list(itertools.chain(
                *list(map(rail.load_all_records, rail.result('get_user_logs'))))))
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test="{{ result('format_logs') | length > 0 }}",
            yes_task='create_csv_lines',
            no_task='delete_this_dagrun'
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source=lambda: rail.result('format_logs'),
            header=[
                    'employeeid',
                    'action',
                    'status',
                    'reason',
                    'job id'],
            row=[
                "{{ item.properties.employeeid }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.get('reason','') }}",
                "{{ item.ecid }}",
            ]
        )

        log_filename = rail.PythonOperator(
            task_id='log_filename',
            python_callable=lambda:  rail.render_template(
                "Log_{{ dag_run_ecid() }}_user_sync.csv")
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name="{{ result('create_csv_lines') }}",
            output_file_name='{{ result("log_filename") }}',
            expires_in_seconds=7*24*60*60,
        )

        def get_severity_logs(severity):
            final_logs = rail.result('format_logs')
            error_logs = rail.find_first_by_attr_and_get_attr(
                final_logs, 'severity', severity, 'properties.employeeid')
            return error_logs

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: get_severity_logs('Error')
        )

        get_exception_logs = rail.PythonOperator(
            task_id="get_exception_logs",
            python_callable=lambda: get_severity_logs('Exception')
        )

        get_warning_logs = rail.PythonOperator(
            task_id="get_warning_logs",
            python_callable=lambda: get_severity_logs('Warning')
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_email",
            to=config.tenant_email,
            bcc='{{ "' +
            config.internal_logs_email +
            '" if result("get_errored_logs", key="length") == 0 else "' +
            config.alert_email +
            '" }}',
            subject='{{ get_company_key() + " | User import - " }} \
                {%- if result("get_errored_logs") | is_truthy -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs") | is_truthy -%} \
                        completed with exceptions \
                    {%- elif result("get_warning_logs") | is_truthy -%} \
                        completed with warnings \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y/%m/%d/%H:%M:%S") }}',
            html_content="templates/emails/email_import_complete.html",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun")

        get_log_dagruns_to_process >> is_log_dagruns_present

        is_log_dagruns_present >> rail.Label(
            'Yes') >> get_user_logs >> format_logs >> has_any_data

        has_any_data >> rail.Label(
            "Yes") >> create_csv_lines >> log_filename >> generate_download_link >> get_errored_logs >> \
            get_exception_logs >> get_warning_logs >> send_import_complete_email

        has_any_data >> rail.Label(
            "No") >> delete_this_dagrun

        is_log_dagruns_present >> rail.Label(
            "No") >> delete_this_dagrun

        return dag


rail.for_each_instance(create_log_airflow_dag)

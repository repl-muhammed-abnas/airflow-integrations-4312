from datetime import timedelta, timezone, datetime
from airflow.models import Variable, DagRun
import rail


# pylint:disable = too-many-statements
def create_log_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_Project_import_master_log_scheduled_{config.instance}',
        description=f'Project_Sync_dynamicwait_Loggeneration_Scheduled {config.instance}',
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
                     f'deltek_costpoint_project_sync_main_{config.instance}']
        )

        is_log_dagruns_present = rail.IfOperator(
            task_id='is_log_dagruns_present',
            test="{{ result('get_log_dagruns_to_process') | length > 0 }}",
            yes_task='get_project_logs',
            no_task='delete_this_dagrun'
        )

        get_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_project_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='format_logs',
            flatten=True
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test="{{ result('get_project_logs') | length > 0 }}",
            yes_task='create_csv_lines',
            no_task='delete_this_dagrun'
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source=lambda: rail.result('get_project_logs'),
            header=['Parent Job ID',
                    'Project ID',
                    'Project Name',
                    'Status',
                    'Details',
                    'Job ID'],
            row=[
                "{{ dag_run_ecid() }}",
                "{{ item.properties.proj_id }}",
                "{{ item.properties.proj_name }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.get('details','') }}",
                "{{ item.ecid }}",
            ]
        )

        log_filename = rail.PythonOperator(
            task_id='log_filename',
            python_callable=lambda:  rail.render_template(
                "Log_{{ dag_run_ecid() }}_project_sync.csv")
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name="{{ result('create_csv_lines') }}",
            output_file_name='{{ result("log_filename") }}',
            expires_in_seconds=7*24*60*60,
        )

        def get_severity_logs(severity):
            final_logs = rail.result('get_project_logs')
            error_logs = rail.find_first_by_attr_and_get_attr(
                final_logs, 'severity', severity, 'properties.proj_id')
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
            config.internal_email +
            '" if result("get_errored_logs", key="length") == 0 else "' +
            config.alert_email +
            '" }}',
            subject='{{ get_company_key() + " | Project import - " }} \
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
            html_content="email_import_complete.html",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun")

        get_log_dagruns_to_process >> is_log_dagruns_present

        is_log_dagruns_present >> rail.Label(
            'Yes') >> get_project_logs >> has_any_data

        has_any_data >> rail.Label(
            "Yes") >> create_csv_lines >> log_filename >> generate_download_link >> get_errored_logs >> \
            get_exception_logs >> get_warning_logs >> send_import_complete_email

        has_any_data >> rail.Label(
            "No") >> delete_this_dagrun

        is_log_dagruns_present >> rail.Label(
            "No") >> delete_this_dagrun

        return dag


rail.for_each_instance(create_log_airflow_dag)

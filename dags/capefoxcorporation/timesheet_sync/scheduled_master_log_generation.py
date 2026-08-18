from datetime import timedelta, datetime
import itertools
import pendulum
from airflow.models import Variable, DagRun
import rail


def create_log_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.log_generation_master_dag_id,
        description=f'Timesheet_Sync_Loggeneration_Scheduled {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.log_generation_dag_interval,
        max_active_tasks=config.dag_max_active_tasks,
        start_date=datetime(2022, 1, 1),
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.log_generation_can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_log_dagruns_to_process'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_log_dagruns_to_process',
            end_task='finish',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        def get_dagruns_to_process(lookup_log_timestamp_var, lookup_log_timestamp_hours, dag_id):
            current_time = pendulum.now(config.time_zone)
            lookup_timestamp_value = Variable.get(
                lookup_log_timestamp_var, default_var=None)
            query_execution_start_date = pendulum.parse(lookup_timestamp_value) if lookup_timestamp_value else (
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
                     config.master_dag_id],
        )

        is_log_dagruns_present = rail.IfOperator(
            task_id='is_log_dagruns_present',
            test="{{ result('get_log_dagruns_to_process') | length > 0 }}",
            yes_task='get_timesheet_logs',
            no_task='delete_this_dagrun'
        )

        get_timesheet_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_timesheet_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='create_log',
            flatten=True
        )

        def format_logs_callable():
            logs = list(itertools.chain(
                *list(map(rail.load_all_records, rail.result('get_timesheet_logs')))))
            error_logs = [log for log in logs if log.get('severity') == 'Error']
            success_logs = [log for log in logs if log.get('severity') == 'Success']
            return {
                'total_record_count': len(logs),
                'success_count': len(success_logs),
                'error_count': len(error_logs),
                'has_errored_logs': len(error_logs) > 0,
                'log_artifact': rail.write_json_artifact(logs)
            }

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=format_logs_callable
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test=lambda: rail.result('format_logs')['total_record_count'] > 0,
            yes_task='create_csv_lines',
            no_task='delete_this_dagrun'
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source=lambda: rail.load_all_records(rail.result('format_logs')['log_artifact']),
            header=['recordUniqId',
                    'action',
                    'status',
                    'processInfo',
                    'additionalInfo',
                    'jobID'],
            row=lambda item: [
                item['properties']['employee_id'],
                item['properties']['action'],
                item['properties']['status'],
                item['properties']['details'],
                item['properties']['timesheet_date'],
                item['ecid'],
            ]
        )

        log_filename = rail.PythonOperator(
            task_id='log_filename',
            python_callable=lambda: rail.render_template(
                "Log_{{ dag_run_ecid() }}_timesheet_sync.csv")
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name="{{ result('create_csv_lines') }}",
            output_file_name='{{ result("log_filename") }}',
            expires_in_seconds=config.download_link_expiration_seconds,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_email",
            to=config.tenant_email,
            bcc='{{ "' +
            config.alert_email +
            '" if result("format_logs")["has_errored_logs"] else "' +
            config.internal_email +
            '" }}',
            subject='{{ get_company_key() + " | Replicon Timesheet Sync to Costpoint - " }} '
                    '{%- if result("format_logs")["has_errored_logs"] -%} '
                    'completed with errors '
                    '{%- else -%} '
                    'completed successfully '
                    '{%- endif -%} '
                    '{{ " - " + current_time("%Y/%m/%d/%H:%M:%S") }}',
            html_content="templates/emails/email_import_complete.html",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun")

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            'No') >> get_log_dagruns_to_process

        get_log_dagruns_to_process >> is_log_dagruns_present

        is_log_dagruns_present >> rail.Label(
            'Yes') >> get_timesheet_logs >> format_logs >> has_any_data

        has_any_data >> rail.Label(
            "Yes") >> create_csv_lines >> log_filename >> generate_download_link >> \
            send_import_complete_email >> finish

        has_any_data >> rail.Label(
            "No") >> delete_this_dagrun

        is_log_dagruns_present >> rail.Label(
            "No") >> delete_this_dagrun

        delete_this_dagrun >> finish

        return dag


rail.for_each_instance(create_log_airflow_dag)

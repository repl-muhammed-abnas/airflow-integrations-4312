from datetime import timedelta
from pendulum import datetime, now
from airflow.models import DagRun
import rail


def create_airflow_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"cobalt_time_sync_logs_master_{config.instance}",
        description="cobalt logs upload and mail",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2023, 9, 13, tz=config.eastern_time_zone)
    ) as dag:

        def get_dagruns_to_process(time_zone, lookup_log_timestamp_hours, dag_id):
            current_time = now(time_zone)
            query_execution_start_date = current_time - \
                timedelta(hours=lookup_log_timestamp_hours)
            dag_runs = []
            for run in DagRun.find(dag_id=dag_id, state="success", execution_start_date=query_execution_start_date):
                dag_runs.append(run.id)
            for run in DagRun.find(dag_id=dag_id, state="failed", execution_start_date=query_execution_start_date):
                dag_runs.append(run.id)
            return dag_runs

        get_log_dagruns_to_process = rail.PythonOperator(
            task_id='get_log_dagruns_to_process',
            python_callable=get_dagruns_to_process,
            op_args=[config.eastern_time_zone, config.log_aggregate_hours,
                     f"cobaltcare_zendesk_to_replicon_timesync_master_{config.instance}"]
        )

        is_log_dagruns_present = rail.IfOperator(
            task_id='is_log_dagruns_present',
            test="{{ result('get_log_dagruns_to_process') | length > 0 }}",
            yes_task='get_time_sync_logs',
            no_task='delete_this_dagrun'
        )

        get_time_sync_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_time_sync_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='cobaltcare_log_lookup_table',
            flatten=True
        )

        def format_logs():
            logs = []
            for log in rail.result('get_time_sync_logs'):
                log_records = rail.load_all_records(log)
                if log_records:
                    logs.extend(log_records)
            return logs

        load_all_logs = rail.PythonOperator(
            task_id="load_all_logs",
            python_callable=format_logs
        )

        create_time_sync_master_log = rail.CreateLogOperator(
            task_id="create_time_sync_master_log"
        )

        write_time_sync_master_log = rail.WriteLogOperator(
            task_id="write_time_sync_master_log",
            log='{{result("create_time_sync_master_log")}}',
            items="{{result('load_all_logs')|to_json}}",
            message="aggregating log",
            severity=lambda item: item['severity'],
            properties=lambda item: {
                'Logtype': item['properties']['Logtype'],
                'User': item['properties']['Username'],
                'Project': item['properties']['Project'],
                'Task': item['properties']['Task'],
                'Time': item['properties']['Time'],
                'Status': item['properties']['Status'],
                'Reason': item['properties']['Reason'],
                'JobID_Reference_internal': item['properties']['Parentjobid'],
            }
        )

        filter_time_sync_exception = rail.FilterLogEntriesOperator(
            task_id="filter_time_sync_exception",
            log='{{result("create_time_sync_master_log")}}',
            severity="Exception"
        )

        if_exceptions_log = rail.IfOperator(
            task_id="if_exceptions_log",
            test='{{result("filter_time_sync_exception") |load_all_records()| length>0}}',
            yes_task="write_logs_to_csv",
            no_task="log_to_sumo"
        )

        write_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="write_logs_to_csv",
            source='{{result("write_time_sync_master_log")}}',
            header=['Logtype', 'User', 'Project', 'Task', 'Time',
                    'Status', 'Reason', 'JobID_Reference_internal'],
            row=['{{ item.properties.Logtype }}', '{{ item.properties.User }}',
                 '{{ item.properties.Project }}', '{{ item.properties.Task }}',
                 '{{ item.properties.Time }}', '{{ item.properties.Status}}',
                 '{{ item.properties.Reason }}', '{{ item.properties.JobID_Reference_internal}}',]
        )

        generate_pre_signed_download_url = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_pre_signed_download_url",
            artifact_name='{{result("write_logs_to_csv")}}',
            output_file_name='{{ current_time_in_specified_tz(fmt="%d-%m-%Y-%H%M%S", tz="US/Eastern") }}' +
            "taskandtimelogs.csv",
            expires_in_seconds=7*24*60*60
        )

        send_error_mail = rail.EmailOperator(
            task_id="send_error_mail",
            subject='{{get_company_key()}}' + " | Task and time sync from Zendesk - " + "completed with exceptions" +
            " - " +
            '{{ current_time_in_specified_tz(fmt="%d-%m-%Y-%H%M%S", tz="US/Eastern") }}',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            html_content="templates/error_mail.html"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )
        get_log_dagruns_to_process >>\
            is_log_dagruns_present >> rail.Label("Yes") >> get_time_sync_logs >> load_all_logs >>\
            create_time_sync_master_log >> write_time_sync_master_log >> \
            filter_time_sync_exception >> if_exceptions_log >> rail.Label("Yes") >> write_logs_to_csv >>\
            generate_pre_signed_download_url >> send_error_mail >> log_to_sumo
        is_log_dagruns_present >> rail.Label("Yes") >> delete_this_dagrun
        if_exceptions_log >> rail.Label("No") >> log_to_sumo >>\
            can_fail_dag >> fail_dagrun
        return dag


rail.for_each_instance(create_airflow_master_dag)

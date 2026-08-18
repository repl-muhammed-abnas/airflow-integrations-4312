from datetime import datetime, timedelta
import json
import rail
from addsystems.project_import.utils import request_payload


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'addsystems_project_sync_webhook_{config.instance}',
        description='Addsystems Project Sync Import',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_project_available = rail.IfOperator(
            task_id='is_project_available',
            test=lambda dag_run: bool(
                dag_run.conf['webhook']['data']['EntryDetails']),
            yes_task="log_file_name"
        )

        log_file_name = rail.PythonOperator(
            task_id='log_file_name',
            python_callable=lambda: 'logs_addsystem_projectsync_' +
            (datetime.now()).strftime("%Y%m%d%M%S")+'.csv'
        )

        process_each_project_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_project_records',
            items=lambda dag_run: [dag_run.conf['webhook']['data']['EntryDetails']] if isinstance(
                dag_run.conf['webhook']['data']['EntryDetails'], (dict)) else dag_run.conf['webhook']['data']['EntryDetails'],
            trigger_dag_id=f"addsystems_project_data_process_each_child_{config.instance}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_process_project_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_project_records",
            dag_runs="{{result('process_each_project_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=['InternalId', 'Status',
                    'Description', 'Entrydate', 'ecid'],
            row=['{{ item.properties.InternalId }}', '{{ item.severity }}',
                 '{{ item.message }}', '{{ item.properties.Projectstartdate }}', '"{{ item.ecid }}"'],
        )

        get_log_data = rail.PythonOperator(
            task_id='get_log_data',
            python_callable=lambda dag_run: json.dumps(
                request_payload.get_log_data(dag_run))
        )

        filter_master_log = rail.FilterLogEntriesOperator(
            task_id='filter_master_log',
            severity='Error',
        )

        send_logs_client_endpoint = rail.SimpleHttpOperator(
            task_id='send_logs_client_endpoint',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint='/Api.Replicon/logs/post-logs',
            headers={
                "Content-Type": 'application/json',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="{{ result('get_log_data') }}",
            extra_options={
                'verify': False
            }

        )

        any_records_failed = rail.IfOperator(
            task_id='any_records_failed',
            test="{{ result('filter_master_log', 'length') > 0 }}",
            yes_task='send_completion_error_mail',
            no_task='send_completion_mail'
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Project Sync to Replicon is completed successfully for the Transmission ID' +
            ' {{dag_run.conf.webhook.data.TransmissionId }} at {{ current_time_in_specified_tz() }}',
            html_content="templates/email/import_complete.html",
            files=[
                ('ProjectSync_' + (datetime.now()).strftime("%m%d%Y%H%M%S")+'.csv', "{{ result('render_logs_csv') }}")]
        )

        send_completion_error_mail = rail.EmailOperator(
            task_id='send_completion_error_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Project Sync to Replicon is completed with error for the Transmission ID' +
            ' {{dag_run.conf.webhook.data.TransmissionId }} at {{ current_time_in_specified_tz() }}',
            html_content="templates/email/import_with_error.html",
            files=[
                ('ProjectSync_' + (datetime.now()).strftime("%m%d%Y%H%M%S")+'.csv', "{{ result('render_logs_csv') }}")]
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        is_project_available >> log_file_name >> process_each_project_records >> wait_process_project_records >> render_logs_csv\
            >> get_log_data >> send_logs_client_endpoint >> filter_master_log >> any_records_failed

        any_records_failed >> rail.Label(
            "Yes") >> send_completion_error_mail >> log_to_sumo
        log_to_sumo >> can_fail_dag >> fail_dagrun

        any_records_failed >> rail.Label(
            "No") >> send_completion_mail >> log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)

from airflow.models import Variable
from datetime import timedelta
import rail

from deltek_vantagepoint.project_sync.utils import python_callable_method


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_{config.region.replace("-", "_")}_project_sync_main_{config.company_key}',
        description=f'Deltek Vantagepoint project and client sync {config.company_key}',
        schedule_interval=None,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            basic_auth_username_var=config.basic_auth_user_var,
            basic_auth_password_var=config.basic_auth_pass_var
        ),
        default_args= { 'vp_conn_id': config.deltek_vantagepoint_conn_id }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_full_sync'
        )


        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_full_sync',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )


        def check_if_full_sync():
            key = f'is_project_full_sync_{config.company_key}'
            value = Variable.get(key, default_var='false').lower() == 'true'
            if value:
                Variable.set(key, False)
            return value
        is_full_sync = rail.IfOperator(
            task_id='is_full_sync',
            test=check_if_full_sync,
            yes_task='fetch_all_vp_projects',
            no_task='is_webhook_invalid'
        )

        is_webhook_invalid = rail.IfOperator(
            task_id='is_webhook_invalid',
            test=lambda dag_run: python_callable_method.get_invalidity_reason(dag_run, config),
            yes_task='write_webhook_exception',
            no_task='is_delete_action'
        )

        write_webhook_exception = rail.WriteLogOperator(
            task_id='write_webhook_exception',
            message='Exceptions',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'code': dag_run.conf['webhook']['data']['WBS1'] \
                    if dag_run.conf.get('webhook', {}).get('data', {}).get('WBS1', False) else '',
                'action': dag_run.conf['webhook']['data']['Action'] \
                    if dag_run.conf.get('webhook', {}).get('data', {}).get('Action', False) else '',
                'status': 'Exception',
                'reason': python_callable_method.get_invalidity_reason(dag_run, config)
            }
        )


        is_delete_action = rail.IfOperator(
            task_id='is_delete_action',
            test=lambda dag_run: dag_run.conf['webhook']['data']['Action'] == config.WEBHOOK_ACTION['DELETE'],
            yes_task='is_phase_or_task',
            no_task='fetch_project_hierarchy'
        )

        is_phase_or_task = rail.IfOperator(
            task_id='is_phase_or_task',
            test="{{ dag_run.conf.webhook.data.WBS2 != ' ' }}",
            yes_task='fetch_project_hierarchy',
            no_task='log_to_sumo'
        )

        fetch_project_hierarchy = rail.VantagepointAPIOperator(
            task_id='fetch_project_hierarchy',
            endpoint='/project/hierarchy/{{ dag_run.conf.webhook.data.WBS1 }}',
            filters=f'?fieldFilter={config.PROJECT_FIELDS}',
            request_method='GET'
        )


        fetch_all_vp_projects = rail.VantagepointAPIOperator(
            task_id = 'fetch_all_vp_projects',
            endpoint='/project',
            filters=f'?fieldFilter={config.PROJECT_FIELDS}',
            request_method='GET'
        )


        is_project_exists = rail.IfOperator(
            task_id='is_project_exists',
            test='{{ get_error_message() | is_falsy }}',
            yes_task='filtered_projects',
            no_task='write_project_exception',
            trigger_rule='all_done'
        )

        write_project_exception = rail.WriteLogOperator(
            task_id='write_project_exception',
            message='Exceptions',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'code': dag_run.conf['webhook']['data']['WBS1'],
                'action': dag_run.conf['webhook']['data']['Action'],
                'status': 'Exception',
                'reason': 'Project does not exist'
            }
        )


        filtered_projects = rail.PythonOperator(
            task_id='filtered_projects',
            python_callable=lambda dag_run: python_callable_method.get_filtered_projects(dag_run, config)
        )

        trigger_project_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_project_sync_child_dag',
            retries=0,
            items=lambda: rail.result('filtered_projects'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_vantagepoint_{config.region.replace("-", "_")}_project_sync_child_{config.company_key}',
            conf=lambda dag_run, item: python_callable_method.get_child_dag_conf(dag_run, item, config)
        )

        wait_for_project_sync_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_project_sync_completion',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_project_sync_child_dag") }}'
        )


        search_exceptions = rail.FilterLogEntriesOperator(
            task_id='search_exceptions',
            severity='Error/Exception'
        )

        if_logs_present = rail.IfOperator(
            task_id='if_logs_present',
            test="{{ result('search_exceptions', 'length') > 0 }}",
            yes_task='write_logs_to_csv',
            no_task='log_to_sumo'
        )

        write_logs_to_csv = rail.WriteCSVFileOperator(
            task_id='write_logs_to_csv',
            source='{{ result("search_exceptions") }}',
            header=['Project Code', 'Action', 'Status', 'Message', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('code','') }}",
                "{{ item.properties | attr_or_default('action','') }}",
                "{{ item.properties | attr_or_default('status','') }}",
                "{{ item.properties | attr_or_default('reason','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('write_logs_to_csv')}}",
            output_file_name='ProjectSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_mail_alert = rail.EmailOperator(
            task_id='send_mail_alert',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Deltek Vantagepoint Project sync Completed with Errors/Exceptions - {{ current_time() }}',
            html_content="templates/failure_email.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )


        can_run_batch_task >> rail.Label('Yes') >> batch_task
        can_run_batch_task >> rail.Label('No') >> is_full_sync

        batch_task >> log_to_sumo
        is_full_sync >> rail.Label('Yes') >> fetch_all_vp_projects >> filtered_projects
        is_full_sync >> rail.Label('No') >> is_webhook_invalid

        is_webhook_invalid >> rail.Label('Yes') >> write_webhook_exception >> search_exceptions
        is_webhook_invalid >> rail.Label('No') >> is_delete_action

        is_delete_action >> rail.Label('Yes') >> is_phase_or_task
        is_delete_action >> rail.Label('No') >> fetch_project_hierarchy

        is_phase_or_task >> rail.Label('No') >> log_to_sumo
        is_phase_or_task >> rail.Label('Yes') >> fetch_project_hierarchy >> is_project_exists

        is_project_exists >> rail.Label('Yes') >> filtered_projects >> trigger_project_sync_child_dag
        is_project_exists >> rail.Label('No') >> write_project_exception >> search_exceptions

        trigger_project_sync_child_dag >> wait_for_project_sync_completion >> search_exceptions >> if_logs_present

        if_logs_present >> rail.Label('Yes') >> write_logs_to_csv >> generate_download_link >> send_mail_alert >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)

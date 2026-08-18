from datetime import timedelta
import rail
from deltek_vantagepoint_v2.initial_setup import utils


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_creation_dag_id,
        description=f'{config.company_key} Vantagepoint Webhook Creation',
        company_key=config.company_key,
        max_active_runs=1,
        multi_tenant=True,
        webhook_conf=rail.WebhookConf(hmac_secret_var=config.hmac_secret)
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_existing_employee_workflows',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_conf(dag_run):
            return dag_run.conf.get('webhook', {}).get('data', dag_run.conf)

        get_existing_employee_workflows = rail.VantagepointAPIOperator(
            task_id='get_existing_employee_workflows',
            vp_conn_id="{{ (dag_run.conf.webhook.data.vantagepoint_conn_id) if 'webhook' in dag_run.conf else dag_run.conf.vantagepoint_conn_id }}",
            endpoint='/Workflow/WorkflowEvents/EmployeeICBO',
            request_method='GET',
            filters='?WorkflowAPP=I&ApplicationName=EmployeeICBO'
        )

        get_existing_project_workflows = rail.VantagepointAPIOperator(
            task_id='get_existing_project_workflows',
            vp_conn_id="{{ (dag_run.conf.webhook.data.vantagepoint_conn_id) if 'webhook' in dag_run.conf else dag_run.conf.vantagepoint_conn_id }}",
            endpoint='/Workflow/WorkflowEvents/ProjectICBO',
            request_method='GET',
            filters='?WorkflowAPP=I&ApplicationName=ProjectICBO'
        )

        get_existing_webhook_actions = rail.VantagepointAPIOperator(
            task_id='get_existing_webhook_actions',
            vp_conn_id="{{ (dag_run.conf.webhook.data.vantagepoint_conn_id) if 'webhook' in dag_run.conf else dag_run.conf.vantagepoint_conn_id }}",
            endpoint='/Workflow/dlgWorkflowActionWebhook',
            request_method='GET'
        )

        def find_existing_workflow(results, company_key, event_type, payload_key):
            description = f"{getattr(config, payload_key)['Description']} - {company_key}"
            if not results:
                return None
            for w in results:
                if w.get('Description') == description and w.get('EventType', '').lower() == event_type:
                    return w
            return None

        def find_existing_action(existing_workflow):
            if not existing_workflow:
                return None
            event_id = existing_workflow.get('EventID')
            all_actions = rail.result('get_existing_webhook_actions') or []
            return next((a for a in all_actions if a.get('ActionID') == event_id), None)

        trigger_employee_webhooks = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_employee_webhooks',
            trigger_dag_id=config.webhook_employee_child_dag_id,
            items=utils.get_event_types,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run, item: {
                'vantagepoint_conn_id': get_conf(dag_run)['vantagepoint_conn_id'],
                'replicon_conn_id': get_conf(dag_run)['replicon_conn_id'],
                'company_key': get_conf(dag_run)['company_key'],
                'event_type': item['event_type'],
                'customSettings': get_conf(dag_run).get('customSettings', {}),
                'existing_workflow': (existing_workflow := find_existing_workflow(
                    rail.result('get_existing_employee_workflows'),
                    get_conf(dag_run)['company_key'],
                    item['event_type'],
                    f"workflow_event_payload_{item['event_type']}"
                )),
                'existing_action': find_existing_action(existing_workflow)
            }
        )

        trigger_project_webhooks = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_project_webhooks',
            trigger_dag_id=config.webhook_project_child_dag_id,
            items=utils.get_event_types,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run, item: {
                'company_key': get_conf(dag_run)['company_key'],
                'vantagepoint_conn_id': get_conf(dag_run)['vantagepoint_conn_id'],
                'replicon_conn_id': get_conf(dag_run)['replicon_conn_id'],
                'event_type': item['event_type'],
                'existing_workflow': (existing_workflow := find_existing_workflow(
                    rail.result('get_existing_project_workflows'),
                    get_conf(dag_run)['company_key'],
                    item['event_type'],
                    f"workflow_event_payload_project_{item['event_type']}"
                )),
                'existing_action': find_existing_action(existing_workflow)
            }
        )

        wait_for_employee_webhooks = rail.WaitForDagRunsSensor(
            task_id='wait_for_employee_webhooks',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_employee_webhooks") }}'
        )

        wait_for_project_webhooks = rail.WaitForDagRunsSensor(
            task_id='wait_for_project_webhooks',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_project_webhooks") }}'
        )

        gather_child_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_dag_errors',
            dag_runs="{{ [result('trigger_employee_webhooks'), result('trigger_project_webhooks')] }}",
            dagrun_task_id='catch_error',
            flatten=True
        )

        should_log_user_sync_error = rail.IfOperator(
            task_id='should_log_user_sync_error',
            test="{{ 'webhook' in dag_run.conf and get_task_state('catch_error') == 'success' }}",
            yes_task='log_webhook_failure_to_user_sync',
            no_task='log_dagrun_details_to_table',
            trigger_rule='all_done'
        )

        log_webhook_failure_to_user_sync = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_webhook_failure_to_user_sync',
            trigger_rule='all_done',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key="{{ dag_run.conf.webhook.data.company_key }}",
            connector_name=config.provider,
            integration_type='user_sync'
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            trigger_rule='all_done',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key="{{ (dag_run.conf.webhook.data.company_key) if 'webhook' in dag_run.conf else dag_run.conf.company_key }}",
            connector_name=config.provider,
            integration_type="{{ 'webhook_subscription' if 'webhook' in dag_run.conf else 'initial_setup' }}"
        )

        is_webhook_error = rail.IfOperator(
            task_id='is_webhook_error',
            test="{{ (get_task_state('gather_child_dag_errors') == 'success' and result('gather_child_dag_errors') | length > 0) }}",
            yes_task='fail_webhook_error',
            no_task='catch_error'
        )

        fail_webhook_error = rail.FailOperator(
            task_id='fail_webhook_error',
            message="{{ result('gather_child_dag_errors') | map_to_attr('error') | join('|') }}"
        )

        def get_webhook_error(error_message):
            return {'error': f'Error in webhook subscription - {error_message}'}

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_webhook_error,
            op_args=['{{ get_error_message() }}']
        )

        should_fail_on_error = rail.IfOperator(
            task_id='should_fail_on_error',
            test="{{ get_task_state('catch_error') == 'success' }}",
            yes_task='fail_dag',
            no_task='should_log_user_sync_error'
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message="{{ result('catch_error')['error'] }}"
        )

        batch_task >> catch_error
        batch_task >> get_existing_employee_workflows >> get_existing_project_workflows >> get_existing_webhook_actions
        get_existing_webhook_actions >> trigger_employee_webhooks >> trigger_project_webhooks >> wait_for_employee_webhooks
        wait_for_employee_webhooks >> wait_for_project_webhooks >> gather_child_dag_errors
        gather_child_dag_errors >> is_webhook_error
        is_webhook_error >> rail.Label('Yes') >> fail_webhook_error >> catch_error
        is_webhook_error >> rail.Label('No') >> catch_error
        catch_error >> should_fail_on_error
        should_fail_on_error >> rail.Label('Yes') >> fail_dag >> should_log_user_sync_error
        should_fail_on_error >> rail.Label('No') >> should_log_user_sync_error
        should_log_user_sync_error >> rail.Label('Yes') >> log_webhook_failure_to_user_sync >> log_dagrun_details_to_table
        should_log_user_sync_error >> rail.Label('No') >> log_dagrun_details_to_table

        return dag


rail.for_each_instance(create_dag)

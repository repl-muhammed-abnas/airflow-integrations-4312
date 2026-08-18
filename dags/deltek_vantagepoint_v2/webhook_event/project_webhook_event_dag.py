from datetime import timedelta
import hashlib
import hmac
import json
import os
import rail
from airflow.models import Variable
from deltek_vantagepoint_v2.main_dag.utils import get_connector_clientids_with_initial_settings

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.project_webhook_event_dag_id,
        description=f'{config.company_key} Vantagepoint Webhook Event Handler for Project Events',
        company_key=config.company_key,
        max_active_runs=10,
        multi_tenant=True,
        webhook_conf=rail.WebhookConf(
            basic_auth_username_var=config.webhook_basicauth_username,
            basic_auth_password_var=config.webhook_basicauth_password
        )
    ) as dag:
        
        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)
        
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_project_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_hmac_signature_and_get_request_body'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_hmac_signature_and_get_request_body',
            end_task='should_delete_dagrun',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_hmac_signature_and_request_body():
            hmac_secret = bytes(Variable.get(config.hmac_secret), 'utf-8')
            body = {
                'connectorName': config.connector_name,
                'environment': os.environ.get('AIRFLOW_ENVIRONMENT', 'dev'),
                'region': os.environ.get('REGION', 'unknown')
            }
            signature = hmac.new(hmac_secret, bytes(json.dumps(
                body, separators=(",", ":")), 'utf-8'), digestmod=hashlib.sha256)
            return {'signature': signature.hexdigest(), 'request_body': json.dumps(body)}

        create_hmac_signature_and_get_request_body = rail.PythonOperator(
            task_id='create_hmac_signature_and_get_request_body',
            python_callable=get_hmac_signature_and_request_body
        )

        get_vantagepoint_company_conn_ids = rail.SimpleHttpOperator(
            task_id='get_vantagepoint_company_conn_ids',
            method='POST',
            http_conn_id=config.airflow_connector_ui_connid,
            endpoint='integration-settings-api/connector-info',
            headers={
                'Content-Type': 'application/json',
                'x-airflow-connectors-signature': "{{ result('create_hmac_signature_and_get_request_body').signature }}"
            },
            data="{{ result('create_hmac_signature_and_get_request_body').request_body }}"
        )

        parse_vantagepoint_clientids = rail.PythonOperator(
            task_id='parse_vantagepoint_clientids',
            python_callable=lambda: get_connector_clientids_with_initial_settings(rail.result(
                'get_vantagepoint_company_conn_ids'), config.workflows)
        )

        is_new_vantagepoint_project_event = rail.IfOperator(
            task_id='is_new_vantagepoint_project_event',
            test=lambda dag_run: (
                dag_run.conf.get('webhook', {}).get('data') is not None
                and len(dag_run.conf.get('webhook', {}).get('data', {})) > 0
            ),
            yes_task='trigger_project_sync',
            no_task='should_delete_dagrun'
        )

        def prepare_project_sync_conf(dag_run):
            webhook_data = dag_run.conf.get('webhook', {}).get('data', {})

            company_key = webhook_data.get('company_key')
            if not company_key:
                raise Exception(f"Missing company_key in webhook data")

            clientids = rail.result('parse_vantagepoint_clientids')
            project_sync_clients = clientids.get('project_sync', [])
            company_config = next(
                (client for client in project_sync_clients if client.get('company_key') == company_key),
                None
            )

            if not company_config:
                raise Exception(f"No configuration found for company_key: {company_key}")

            vantagepoint_conn_id = company_config.get('vantagepoint_conn_id')
            replicon_conn_id = company_config.get('replicon_conn_id')

            return {
                'company_key': company_key,
                'vantagepoint_conn_id': vantagepoint_conn_id,
                'replicon_conn_id': replicon_conn_id,
                'triggered_by': 'webhook',
                'customSettings': company_config.get('customSettings', {}),
                'initial_custom_settings': company_config.get('initial_custom_settings', {}),
                'webhook': {
                    'data': webhook_data
                }
            }

        trigger_project_sync = rail.TriggerDagRunOperator(
            task_id='trigger_project_sync',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.project_sync_main_dag_id,
            conf=prepare_project_sync_conf
        )

        should_delete_dagrun = rail.IfOperator(
            task_id='should_delete_dagrun',
            test="{{ get_task_state('trigger_project_sync') == 'skipped' }}",
            trigger_rule='all_done',
            yes_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> should_delete_dagrun
        can_run_batch_task >> rail.Label('No') >> create_hmac_signature_and_get_request_body
        create_hmac_signature_and_get_request_body >> get_vantagepoint_company_conn_ids
        get_vantagepoint_company_conn_ids >> parse_vantagepoint_clientids
        parse_vantagepoint_clientids >> is_new_vantagepoint_project_event

        is_new_vantagepoint_project_event >> rail.Label('Yes') >> trigger_project_sync
        is_new_vantagepoint_project_event >> rail.Label('No') >> should_delete_dagrun

        trigger_project_sync >> should_delete_dagrun
        should_delete_dagrun >> rail.Label('Yes') >> delete_this_dagrun

        return dag

rail.for_each_instance(create_dag)
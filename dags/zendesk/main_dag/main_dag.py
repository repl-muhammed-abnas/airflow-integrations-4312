from datetime import timedelta
import hashlib
import json
import hmac
import os
import pendulum
import rail
from airflow.models import Variable

null = None


def create_main_dag(config):
    # pylint: disable = too-many-statements
    with rail.create_airflow_dag(
        dag_id=f"standard_zendesk_{config.region.replace('-', '_')}_main_trigger_{config.instance}",
        description=f'Zendesk {config.region} Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 4, 1, tz=config.timezone_iana),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_hmac_signature_and_get_request_body'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_hmac_signature_and_get_request_body',
            end_task='should_delete_dagrun',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def get_hmac_signature_and_request_body():
            hmac_secret = bytes(Variable.get(config.hmac_secret), 'utf-8')
            body = {
                'connectorName': config.connector_name,
                'environment': os.environ.get('AIRFLOW_ENVIRONMENT', 'dev'),
                'region': os.environ.get('REGION', 'dev')
            }
            signature = hmac.new(hmac_secret, bytes(json.dumps(
                body, separators=(",", ":")), 'utf-8'), digestmod=hashlib.sha256)
            return {'signature': signature.hexdigest(), 'request_body': json.dumps(body)}
        create_hmac_signature_and_get_request_body = rail.PythonOperator(
            task_id='create_hmac_signature_and_get_request_body',
            python_callable=get_hmac_signature_and_request_body
        )

        get_zendesk_company_conn_ids = rail.SimpleHttpOperator(
            task_id="get_zendesk_company_conn_ids",
            method="POST",
            http_conn_id=config.airflow_connector_ui_connid,
            endpoint="integration-settings-api/connector-info",
            headers={
                "Content-Type": "application/json",
                "x-airflow-connectors-signature": "{{ result('create_hmac_signature_and_get_request_body').signature }}",
            },
            data="{{ result('create_hmac_signature_and_get_request_body').request_body }}",
        )

        def get_zendesk_clientids_by_integration():
            client_import_list = []
            project_import_list =[]
            custom_integrations = []

            # Load xero company connections
            zendesk_company_connids = json.loads(
                rail.result("get_zendesk_company_conn_ids")
            )

            # Define workflow IDs
            client_import = config.client_import
            project_import = config.project_import

            # Iterate through zendesk company connections
            for each_client in zendesk_company_connids:
                # Extract dag settings
                dag_settings = each_client.pop('dag_settings')

                if dag_settings:
                    # Handle client import workflow
                    client_import_workflow = next(
                        iter(filter(lambda x: not(x.get('isCustom')) and x['workflowId'] == client_import and x['enabled'].lower() == 'yes', dag_settings)), null)
                    if client_import_workflow:
                        client_import_list.append(
                            {**each_client, 'customSettings': client_import_workflow['customSettings']})
                    # Handle project import workflow
                    project_import_workflow = next(
                        iter(filter(lambda x: not(x.get('isCustom')) and    x['workflowId'] == project_import and x['enabled'].lower() == 'yes', dag_settings)), null)
                    if project_import_workflow:
                        project_import_list.append(
                            {**each_client, 'customSettings': project_import_workflow['customSettings']})
                    #Handle customized integration workflows
                    custom_integration_workflows = map(lambda y, ec = each_client: {**ec, 'dagId': y['isCustom'],
                        'customSettings': y['customSettings']}, filter(lambda x: x.get('isCustom') and x['enabled'].lower() == 'yes', dag_settings))
                    if custom_integration_workflows:
                        custom_integrations.extend(custom_integration_workflows)

            return {
                f"{client_import}": client_import_list,
                f"{project_import}": project_import_list,
                "custom_integrations": custom_integrations
            }

        parse_zendesk_clientids = rail.PythonOperator(
            task_id="parse_zendesk_clientids",
            python_callable=lambda: rail.get_connector_clientids_by_integration(rail.result(
                'get_zendesk_company_conn_ids'), config.workflows)
        )

        is_client_import = rail.IfOperator(
            task_id='is_client_import',
            test=lambda: len(rail.result('parse_zendesk_clientids')[
                             config.client_import]) > 0,
            yes_task='trigger_client_import',
            no_task='is_project_import'
        )

        trigger_client_import = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_client_import",
            trigger_dag_id=config.client_import_dag,
            retries=0,
            items=lambda: rail.result("parse_zendesk_clientids")[config.client_import],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items()),
        )

        is_project_import = rail.IfOperator(
            task_id="is_project_import",
            test=lambda: len(
                rail.result("parse_zendesk_clientids")[config.project_import]
            )
            > 0,
            yes_task="trigger_project_import",
            no_task='is_custom_integrations_present'
        )

        trigger_project_import = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_project_import",
            trigger_dag_id=config.project_import_dag,
            retries=0,
            items=lambda: rail.result("parse_zendesk_clientids")[config.project_import],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items()),
        )

        is_custom_integrations_present = rail.IfOperator(
            task_id='is_custom_integrations_present',
            test=lambda: len(rail.result('parse_zendesk_clientids')[
                'custom_integrations']) > 0,
            yes_task='trigger_custom_integrations',
            no_task='should_delete_dagrun'
        )

        trigger_custom_integrations = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_custom_integrations',
            trigger_dag_id=lambda item: item['dagId'],
            retries=0,
            items=lambda: rail.result('parse_zendesk_clientids')[
                'custom_integrations'],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        should_delete_dagrun = rail.IfOperator(
            task_id='should_delete_dagrun',
            test="{{ get_task_state('trigger_client_import') == 'skipped' and \
                    get_task_state('trigger_project_import') == 'skipped' and \
                    get_task_state('trigger_custom_integrations') == 'skipped' }}",
            trigger_rule='all_done',
            yes_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_delete_dagrun
        can_run_batch_task >> rail.Label(
            'No') >> create_hmac_signature_and_get_request_body
        create_hmac_signature_and_get_request_body >> get_zendesk_company_conn_ids >> parse_zendesk_clientids >> \
            is_client_import
        is_client_import >> rail.Label(
            'Yes') >> trigger_client_import >> is_project_import
        is_client_import >> rail.Label("No") >> is_project_import
        is_project_import >> rail.Label(
            'Yes') >> trigger_project_import
        trigger_project_import >> is_custom_integrations_present
        is_project_import >> rail.Label(
            'No') >> is_custom_integrations_present
        is_custom_integrations_present >> rail.Label('Yes') >> trigger_custom_integrations >> should_delete_dagrun
        is_custom_integrations_present >> rail.Label('No') >> should_delete_dagrun
        should_delete_dagrun >> rail.Label(
            'Yes') >> delete_this_dagrun

        return dag


rail.for_each_instance(create_main_dag)

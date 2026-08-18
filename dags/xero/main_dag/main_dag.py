from datetime import timedelta
import hashlib
import json
import hmac
import os
import pendulum
import rail
from airflow.models import Variable


# pylint: disable = too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_xero_{config.region.replace('-', '_')}_main_trigger_{config.instance}",
        description=f'Xero {config.region} Master {config.instance}',
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

        get_xero_company_conn_ids = rail.SimpleHttpOperator(
            task_id='get_xero_company_conn_ids',
            method='POST',
            http_conn_id=config.airflow_connector_ui_connid,
            endpoint='integration-settings-api/connector-info',
            headers={
                'Content-Type': 'application/json',
                'x-airflow-connectors-signature': "{{ result('create_hmac_signature_and_get_request_body').signature }}"
            },
            data="{{ result('create_hmac_signature_and_get_request_body').request_body }}"
        )

        def get_xero_clientids_by_integration():
            null = None
            client_import = []
            client_export = []
            invoice_export = []
            invoice_status_update_billed = []
            invoice_status_update_paid = []
            custom_integrations = []

            # Load xero company connections
            xero_company_connids = json.loads(
                rail.result('get_xero_company_conn_ids'))

            # Define workflow IDs
            import_client = config.import_client
            export_client = config.export_client
            invoice = config.invoice
            billed_status_update = config.billed_status_update
            paid_status_update = config.paid_status_update

            # Iterate through xero company connections
            for each_client in xero_company_connids:
                # add default notification email if not present
                if not each_client.get('notification_email'):
                    each_client['notification_email'] = f"rit.internallogs+{each_client['company_key']}@replicon.com"
                # Extract dag settings
                dag_settings = each_client.pop('dag_settings')

                if dag_settings:
                    # Handle client import workflow
                    client_import_workflow = next(
                        iter(filter(lambda x: not(x.get('isCustom')) and x['workflowId'] == import_client and x['enabled'].lower() == 'yes',
                            dag_settings)), null)
                    if client_import_workflow:
                        client_import.append(
                            {**each_client, 'customSettings': client_import_workflow['customSettings']})
                    # Handle client export workflow
                    client_export_workflow = next(
                        iter(filter(lambda x: not(x.get('isCustom')) and x['workflowId'] == export_client and x['enabled'].lower() == 'yes',
                            dag_settings)), null)
                    if client_export_workflow:
                        client_export.append(
                            {**each_client, 'customSettings': client_export_workflow['customSettings']})
                    # Handle invoice export workflow
                    invoice_export_workflow = next(
                        iter(filter(lambda x: not(x.get('isCustom')) and x['workflowId'] == invoice and x['enabled'].lower() == 'yes',
                            dag_settings)), null)
                    if invoice_export_workflow:
                        invoice_export.append(
                            {**each_client, 'customSettings': invoice_export_workflow['customSettings']})
                    # Handle billed invoice status update workflow
                    billed_invoice_status_update_workflow = next(
                        iter(filter(lambda x: not(x.get('isCustom')) and x['workflowId'] == billed_status_update and x['enabled'].lower() == 'yes',
                            dag_settings)), null)
                    if billed_invoice_status_update_workflow:
                        invoice_status_update_billed.append(
                            {**each_client, 'customSettings': billed_invoice_status_update_workflow['customSettings']})
                    # Handle paid invoice status update workflow
                    paid_invoice_status_update_workflow = next(
                        iter(filter(lambda x: not(x.get('isCustom')) and x['workflowId'] == paid_status_update and x['enabled'].lower() == 'yes',
                            dag_settings)), null)
                    if paid_invoice_status_update_workflow:
                        invoice_status_update_paid.append(
                            {**each_client, 'customSettings': paid_invoice_status_update_workflow['customSettings']})
                    #Handle customized integration workflows
                    custom_integration_workflows = map(lambda y, ec = each_client: {**ec, 'dagId': y['isCustom'],
                        'customSettings': y['customSettings']}, filter(lambda x: x.get('isCustom') and x['enabled'].lower() == 'yes', dag_settings))
                    if custom_integration_workflows:
                        custom_integrations.extend(custom_integration_workflows)

            return {
                f'{import_client}': client_import,
                f'{export_client}': client_export,
                f'{invoice}': invoice_export,
                f'{billed_status_update}': invoice_status_update_billed,
                f'{paid_status_update}': invoice_status_update_paid,
                'custom_integrations': custom_integrations
            }

        parse_xero_clientids = rail.PythonOperator(
            task_id='parse_xero_clientids',
            python_callable=lambda: rail.get_connector_clientids_by_integration(rail.result(
                'get_xero_company_conn_ids'), config.workflows)
        )

        is_client_import = rail.IfOperator(
            task_id='is_client_import',
            test=lambda: len(rail.result('parse_xero_clientids')[
                             config.import_client]) > 0,
            yes_task='trigger_client_import',
            no_task='is_client_export'
        )

        trigger_client_import = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_client_import',
            trigger_dag_id=config.client_import_dag,
            retries=0,
            items=lambda: rail.result('parse_xero_clientids')[
                config.import_client],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_client_export = rail.IfOperator(
            task_id='is_client_export',
            test=lambda: len(rail.result('parse_xero_clientids')[
                             config.export_client]) > 0,
            yes_task='trigger_client_export',
            no_task='is_invoice_export'
        )

        trigger_client_export = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_client_export',
            trigger_dag_id=config.client_export_dag,
            retries=0,
            items=lambda: rail.result('parse_xero_clientids')[
                config.export_client],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_invoice_export = rail.IfOperator(
            task_id='is_invoice_export',
            test=lambda: len(rail.result('parse_xero_clientids')[
                             config.invoice]) > 0,
            yes_task='trigger_invoice_export',
            no_task='is_billed_status_update_for_invoice'
        )

        trigger_invoice_export = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_invoice_export',
            trigger_dag_id=config.invoice_export_dag,
            retries=0,
            items=lambda: rail.result('parse_xero_clientids')[config.invoice],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_billed_status_update_for_invoice = rail.IfOperator(
            task_id='is_billed_status_update_for_invoice',
            test=lambda: len(rail.result('parse_xero_clientids')[
                             config.billed_status_update]) > 0,
            yes_task='trigger_billed_status_update_for_invoice',
            no_task='is_paid_status_update_for_invoice'
        )

        trigger_billed_status_update_for_invoice = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_billed_status_update_for_invoice',
            trigger_dag_id=config.billed_status_update_dag,
            retries=0,
            items=lambda: rail.result('parse_xero_clientids')[
                config.billed_status_update],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_paid_status_update_for_invoice = rail.IfOperator(
            task_id='is_paid_status_update_for_invoice',
            test=lambda: len(rail.result('parse_xero_clientids')[
                             config.paid_status_update]) > 0,
            yes_task='trigger_paid_status_update_for_invoice',
            no_task='is_custom_integrations_present'
        )

        trigger_paid_status_update_for_invoice = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_paid_status_update_for_invoice',
            trigger_dag_id=config.paid_status_update_dag,
            retries=0,
            items=lambda: rail.result('parse_xero_clientids')[
                config.paid_status_update],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_custom_integrations_present = rail.IfOperator(
            task_id='is_custom_integrations_present',
            test=lambda: len(rail.result('parse_xero_clientids')[
                'custom_integrations']) > 0,
            yes_task='trigger_custom_integrations',
            no_task='should_delete_dagrun'
        )

        trigger_custom_integrations = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_custom_integrations',
            trigger_dag_id=lambda item: item['dagId'],
            retries=0,
            items=lambda: rail.result('parse_xero_clientids')[
                'custom_integrations'],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        should_delete_dagrun = rail.IfOperator(
            task_id='should_delete_dagrun',
            test="{{ get_task_state('trigger_client_import') == 'skipped' and \
                get_task_state('trigger_client_export') == 'skipped' and \
                get_task_state('trigger_invoice_export') == 'skipped' and \
                get_task_state('trigger_billed_status_update_for_invoice') == 'skipped' and \
                get_task_state('trigger_paid_status_update_for_invoice') == 'skipped' and \
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
        create_hmac_signature_and_get_request_body >> get_xero_company_conn_ids >> parse_xero_clientids >> \
            is_client_import
        is_client_import >> rail.Label(
            'Yes') >> trigger_client_import >> is_client_export
        is_client_import >> rail.Label(
            'No') >> is_client_export
        is_client_export >> rail.Label(
            'Yes') >> trigger_client_export >> is_invoice_export
        is_client_export >> rail.Label(
            'No') >> is_invoice_export
        is_invoice_export >> rail.Label(
            'Yes') >> trigger_invoice_export >> is_billed_status_update_for_invoice
        is_invoice_export >> rail.Label(
            'No') >> is_billed_status_update_for_invoice
        is_billed_status_update_for_invoice >> rail.Label(
            'Yes') >> trigger_billed_status_update_for_invoice >> is_paid_status_update_for_invoice
        is_billed_status_update_for_invoice >> rail.Label(
            'No') >> is_paid_status_update_for_invoice
        is_paid_status_update_for_invoice >> rail.Label(
            'Yes') >> trigger_paid_status_update_for_invoice
        trigger_paid_status_update_for_invoice >> is_custom_integrations_present
        is_paid_status_update_for_invoice >> rail.Label(
            'No') >> is_custom_integrations_present
        is_custom_integrations_present >> rail.Label('Yes') >> trigger_custom_integrations >> should_delete_dagrun
        is_custom_integrations_present >> rail.Label('No') >> should_delete_dagrun
        should_delete_dagrun >> rail.Label(
            'Yes') >> delete_this_dagrun

        return dag


rail.for_each_instance(create_main_dag)

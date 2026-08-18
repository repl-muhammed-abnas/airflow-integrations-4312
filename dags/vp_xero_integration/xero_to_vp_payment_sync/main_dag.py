"""
Main (scheduler) DAG for Xero -> VP Payment Sync.

Runs on a configurable schedule (Variable override, default */5 * * * *),
fetches enabled tenants from the middleware, and triggers one dispatcher
DAG run per tenant.

Ports the Workato `014_501_psa` payment_sync poll trigger (tenant fan-out
stage). Payment routing happens in the dispatcher; per-payment processing
happens in the processor DAGs.
"""
# pylint: disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from airflow.models import Variable


def create_dag(config):
    """Per-instance scheduler: fetch enabled tenants, trigger dispatcher per tenant."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_payment_sync_main_{config.instance}',
        description='Scheduler DAG for Xero -> VP Payment Sync',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=Variable.get(
            f'vp_xero_payment_sync_schedule_interval_{config.instance}',
            default_var='*/5 * * * *'
        ),
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_xero', 'payment_sync', 'main'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        get_middleware_auth_token = rail.SimpleHttpOperator(
            task_id='get_middleware_auth_token',
            method='POST',
            http_conn_id=config.middleware_conn_id,
            endpoint='/api/v1/oauth/token',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data={
                'grant_type': 'client_credentials',
                'client_id': "{{ var.value.get('vantagepoint_client_id', '') }}",
                'client_secret': "{{ var.value.get('vantagepoint_client_secret', '') }}",
            },
            response_filter=lambda response: response.json().get('access_token')
        )

        fetch_customers_by_integration = rail.SimpleHttpOperator(
            task_id='fetch_customers_by_integration',
            method='GET',
            http_conn_id=config.middleware_conn_id,
            endpoint='/api/v1/integrations',
            headers={
                'Content-Type': 'application/json',
                'Authorization': "Bearer {{ result('get_middleware_auth_token') }}",
            },
            data={
                'dag_id': f'vp_xero_payment_sync_dispatcher_{config.instance}',
                'integration_type': 'payment_sync',
                'status': 'enabled'
            },
            response_filter=lambda response: response.json().get('integrations', [])
        )

        process_customers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_customers',
            items=lambda: rail.result('fetch_customers_by_integration'),
            trigger_dag_id=f'vp_xero_payment_sync_dispatcher_{config.instance}',
            conf=lambda item: {
                **item,
                'clientId': Variable.get('vantagepoint_client_id'),
                'customerId': item.get('customer_id'),
                'integrationType': item.get('integration_type'),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        (get_middleware_auth_token >> fetch_customers_by_integration >> process_customers)
        return dag


rail.for_each_instance(create_dag)

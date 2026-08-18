"""
Main (scheduler) DAG for QBO -> VP Invoice Payment Sync.

Runs on cron, fetches enabled tenants from the integrations-platform-api
middleware, and triggers one dispatcher DAG run per tenant.
Mirrors `vp_quickbooks_integration/customer_sync/main_dag.py`.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
import os
from datetime import timedelta
from airflow.models import Variable
import rail


def _schedule_for(instance):
    env_key = (
        f"AIRFLOW_VAR_VP_QBO_INVOICE_PAYMENT_SYNC_SCHEDULE_"
        f"{instance.upper()}"
    )
    return os.environ.get(env_key, '*/30 * * * *')


def create_dag(config):
    """Top-level scheduler: middleware tenant fetch -> per-tenant fanout."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_invoice_payment_sync_main_{config.instance}',
        description='Scheduler DAG for QBO -> VP Invoice Payment Sync',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=_schedule_for(config.instance),
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_quickbooks', 'invoice_payment_sync', 'main'],
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
                'client_id': "{{ var.value.vantagepoint_client_id }}",
                'client_secret': "{{ var.value.vantagepoint_client_secret }}"
            },
            response_filter=lambda response: response.json()['access_token']
        )

        fetch_customers_by_integration = rail.SimpleHttpOperator(
            task_id='fetch_customers_by_integration',
            method='GET',
            http_conn_id=config.middleware_conn_id,
            endpoint='/api/v1/integrations',
            headers={
                'Content-Type': 'application/json',
                'Authorization': (
                    "Bearer {{ result('get_middleware_auth_token') }}"
                )
            },
            data={
                'dag_id': (
                    f'vp_qbo_invoice_payment_sync_dispatcher_{config.instance}'
                ),
                'integration_type': 'invoice_payment_sync',
                'status': 'enabled'
            },
            response_filter=lambda response: response.json()['integrations']
        )

        def _build_dispatcher_conf(item):
            conns = item.get('connections') or {}
            required = ('intuit', 'vantagepoint')
            missing = [k for k in required if not conns.get(k)]
            if missing:
                raise ValueError(
                    f"Middleware integration item for customer_id="
                    f"{item.get('customer_id')!r} is missing required "
                    f"connections: {missing}. Got keys={list(conns)}."
                )
            return {
                **item,
                'clientId': Variable.get(
                    'vantagepoint_client_id', default_var=''
                ),
                'customerId': item.get('customer_id'),
                'integrationType': item.get('integration_type'),
                'connections': {
                    'intuit': conns['intuit'],
                    'vantagepoint': conns['vantagepoint'],
                },
            }

        process_customers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_customers',
            items=lambda: rail.result('fetch_customers_by_integration'),
            trigger_dag_id=(
                f'vp_qbo_invoice_payment_sync_dispatcher_{config.instance}'
            ),
            conf=_build_dispatcher_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        (
            get_middleware_auth_token >>
            fetch_customers_by_integration >>
            process_customers
        )

        return dag


rail.for_each_instance(create_dag)

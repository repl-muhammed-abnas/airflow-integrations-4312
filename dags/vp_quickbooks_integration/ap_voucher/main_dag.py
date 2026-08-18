"""
Main DAG for VP -> QBO AP Voucher Sync (region-agnostic).

Scheduled cron entry point. Fetches enabled tenants from the middleware and
triggers the single dispatcher DAG per tenant. The integration record carried
in each tenant's conf includes the `config.CFG_Region` field, which the
dispatcher uses to route vouchers to the correct region processor (US vs
CA-UK). Mirrors `journal_entry_sync/main_dag.py`.
"""
from datetime import timedelta
from airflow.models import Variable
import rail


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """Create main scheduler DAG for VP -> QBO AP Voucher Sync."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_ap_voucher_sync_main_{config.instance}',
        description='Scheduler DAG for VP -> QBO AP Voucher Sync',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=Variable.get(
            f'vp_qbo_ap_voucher_sync_schedule_interval_{config.instance}',
            '*/5 * * * *'  # By default, it runs every 5 minutes
        ),
        max_active_runs=config.max_active_runs,
        tags=[
            'vantagepoint_quickbooks',
            'ap_voucher_sync',
            'main',
        ],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        get_middleware_auth_token = rail.SimpleHttpOperator(
            task_id='get_middleware_auth_token',
            method='POST',
            http_conn_id=config.middleware_conn_id,
            endpoint='/api/v1/oauth/token',
            headers={
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            data={
                'grant_type': 'client_credentials',
                'client_id': Variable.get(
                    'vantagepoint_client_id', default_var=''
                ),
                'client_secret': Variable.get(
                    'vantagepoint_client_secret', default_var=''
                )
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
                'Authorization': (
                    "Bearer {{ result('get_middleware_auth_token') }}"
                )
            },
            data={
                'dag_id': (
                    f'vp_qbo_ap_voucher_sync_dispatcher_{config.instance}'
                ),
                'integration_type': 'ap_voucher_sync',
                'status': 'enabled'
            },
            response_filter=lambda response: (
                response.json().get('integrations', [])
            )
        )

        process_customers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_customers',
            items=lambda: rail.result('fetch_customers_by_integration'),
            trigger_dag_id=(
                f'vp_qbo_ap_voucher_sync_dispatcher_{config.instance}'
            ),
            conf=lambda item: {
                **item,
                'clientId': Variable.get('vantagepoint_client_id'),
                'customerId': item.get('customer_id'),
                'integrationType': item.get('integration_type'),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        (
            get_middleware_auth_token >>
            fetch_customers_by_integration >>
            process_customers
        )

        return dag


rail.for_each_instance(create_dag)

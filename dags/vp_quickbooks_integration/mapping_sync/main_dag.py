"""
Main DAG for VP QBO Mapping Sync.

Scheduled per-instance. Fetches the list of enabled mapping_sync customers
from the middleware, then triggers the dispatcher DAG once per customer.
Mirrors vendor_sync/main_dag.py exactly so the operational shape of the two
integrations stays consistent.
"""
from datetime import timedelta
from airflow.models import Variable
import rail

from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """Create scheduled main DAG for one instance."""
    with rail.create_airflow_dag(
        dag_id=IntegrationConfig.dag_id('main', config.instance),
        description='Processor DAG for VP QBO Mapping Sync',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=Variable.get(
            f'vp_qbo_mapping_sync_schedule_interval_{config.instance}',
            config.mapping_population_schedule,  # per-instance default
        ),
        max_active_runs=config.max_active_runs_master,
        tags=['vantagepoint_quickbooks', 'mapping_sync', 'main'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        get_middleware_auth_token = rail.SimpleHttpOperator(
            task_id='get_middleware_auth_token',
            method='POST',
            http_conn_id=config.middleware_conn_id,
            endpoint='/api/v1/oauth/token',
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            data={
                'grant_type': 'client_credentials',
                # `default_var=''` keeps the DAG parseable in test / CI
                # environments where these Variables aren't seeded
                # (mirrors vendor_sync/main_dag.py line 46). Real
                # deployments seed both vantagepoint_client_id and
                # vantagepoint_client_secret at provisioning time, so
                # the default never gets used in production. Without
                # default_var, parse-time Variable.get raises KeyError
                # and the whole DAG file fails to import.
                'client_id': Variable.get(
                    'vantagepoint_client_id', default_var=''
                ),
                'client_secret': Variable.get(
                    'vantagepoint_client_secret', default_var=''
                ),
            },
            response_filter=lambda response: response.json().get('access_token'),
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
                ),
            },
            data={
                'dag_id': IntegrationConfig.dag_id('dispatcher', config.instance),
                'integration_type': 'mapping_sync',
                'status': 'enabled',
            },
            response_filter=lambda response: (
                response.json().get('integrations', [])
            ),
        )

        process_customers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_customers',
            items=lambda: rail.result('fetch_customers_by_integration'),
            trigger_dag_id=IntegrationConfig.dag_id(
                'dispatcher', config.instance),
            conf=lambda item: {
                **item,
                'clientId': Variable.get('vantagepoint_client_id'),
                'customerId': item.get('customer_id'),
                'integrationType': item.get('integration_type'),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        (
            get_middleware_auth_token >>
            fetch_customers_by_integration >>
            process_customers
        )

        return dag


rail.for_each_instance(create_dag)

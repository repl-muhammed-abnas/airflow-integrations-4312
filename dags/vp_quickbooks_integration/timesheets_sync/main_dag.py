"""
Main (scheduler) DAG for VP -> QBO Timesheets Sync.

Runs on cron, fetches enabled tenants from the integrations-platform-api
middleware, and triggers one dispatcher DAG run per tenant.
Mirrors `vp_ukgpro_integration/employee_sync/main_dag.py`.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
from airflow.models import Variable
import rail
from vp_quickbooks_integration.timesheets_sync.config import (
    default_schedule_interval,
)


def create_dag(config):
    """Top-level scheduler: middleware tenant fetch -> per-tenant fanout."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_timesheets_sync_main_{config.instance}',
        description='Scheduler DAG for VP -> QBO Timesheets Sync',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=Variable.get(
            f'vp_qbo_timesheets_sync_schedule_interval_'
            f'{config.instance}',
            default_schedule_interval,
        ),
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_quickbooks', 'timesheets_sync', 'main'],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
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
                    f'vp_qbo_timesheets_sync_dispatcher_{config.instance}'
                ),
                'integration_type': 'timesheets_sync',
                'status': 'enabled'
            },
            response_filter=lambda response: response.json()['integrations']
        )

        process_customers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_customers',
            items=lambda: rail.result('fetch_customers_by_integration'),
            trigger_dag_id=(
                f'vp_qbo_timesheets_sync_dispatcher_'
                f'{config.instance}'
            ),
            conf=lambda item: {
                **item,
                'clientId': Variable.get('vantagepoint_client_id'),
                'customerId': item.get('customer_id'),
                'integrationType': item.get('integration_type'),
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        (
            get_middleware_auth_token >>
            fetch_customers_by_integration >>
            process_customers
        )

        return dag


rail.for_each_instance(create_dag)

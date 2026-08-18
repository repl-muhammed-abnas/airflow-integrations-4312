"""
Scheduler DAG for VP UKG Pro Journal Sync.
"""
from datetime import timedelta
from airflow.models import Variable
import rail


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """
    Create scheduler DAG for VP UKG Pro Journal Sync.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_ukgpro_journal_sync_scheduler_{config.instance}',
        description='Scheduler DAG for VP UKG Pro Journal Sync',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=Variable.get(
            f'vp_ukgpro_journal_sync_schedule_interval_{config.instance}',
            '0 0 * * *'  # By default, it runs every day at 12 AM
        ),
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_ukgpro', 'journal_sync', 'scheduler'],
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
                'integration_type': 'journal_sync',
                'status': 'enabled'
            },
            response_filter=lambda response: response.json()['integrations']
        )

        process_customers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_customers',
            items=lambda: rail.result('fetch_customers_by_integration'),
            trigger_dag_id=(
                f'vp_ukgpro_journal_sync_main_{config.instance}'
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

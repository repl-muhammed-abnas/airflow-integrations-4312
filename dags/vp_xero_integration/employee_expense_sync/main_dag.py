"""Main DAG for VP -> Xero Employee Expense Sync."""

# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
from datetime import timedelta
from airflow.models import Variable
import rail
from vp_xero_integration.employee_expense_sync import config as sync_config


def create_dag(config):
    """Create main scheduler DAG for VP -> Xero Employee Expense Sync."""
    with rail.create_airflow_dag(
        dag_id=f'{sync_config.main_dag_id_prefix}_{config.instance}',
        description=sync_config.main_dag_description,
        integration_type=sync_config.integration_type,
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=Variable.get(
            f'{sync_config.schedule_interval_variable_key_prefix}'
            f'_{config.instance}',
            sync_config.default_schedule_interval,
        ),
        max_active_runs=config.max_active_runs,
        tags=sync_config.main_dag_tags,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        get_middleware_auth_token = rail.SimpleHttpOperator(
            task_id='get_middleware_auth_token',
            method='POST',
            http_conn_id=config.middleware_conn_id,
            endpoint=sync_config.middleware_auth_endpoint,
            headers={
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            data={
                'grant_type': 'client_credentials',
                'client_id': Variable.get(
                    sync_config.vantagepoint_client_id_variable_key,
                    default_var='',
                ),
                'client_secret': Variable.get(
                    sync_config.vantagepoint_client_secret_variable_key,
                    default_var='',
                )
            },
            response_filter=lambda response: response.json().get('access_token')
        )

        fetch_customers_by_integration = rail.SimpleHttpOperator(
            task_id='fetch_customers_by_integration',
            method='GET',
            http_conn_id=config.middleware_conn_id,
            endpoint=sync_config.middleware_integrations_endpoint,
            headers={
                'Content-Type': 'application/json',
                'Authorization': (
                    "Bearer {{ result('get_middleware_auth_token') }}"
                )
            },
            data={
                'dag_id': (
                    f'{sync_config.dispatcher_dag_id_prefix}'
                    f'_{config.instance}'
                ),
                'integration_type': sync_config.middleware_integration_type,
                'status': sync_config.middleware_integration_status_filter,
            },
            response_filter=lambda response: (
                response.json().get('integrations', [])
            )
        )

        process_customers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_customers',
            items=lambda: rail.result('fetch_customers_by_integration'),
            trigger_dag_id=(
                f'{sync_config.dispatcher_dag_id_prefix}_{config.instance}'
            ),
            conf=lambda item: {
                **item,
                'clientId': Variable.get(
                    sync_config.vantagepoint_client_id_variable_key
                ),
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

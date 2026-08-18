"""
Main DAG for Xero -> VP Tax Code Schedule.

Hourly scheduler ported from Workato recipe ``014_501_psa_schedule_xero_tax_codes``.
On each tick it fetches every enabled customer from the middleware and triggers
one ``vp_xero_tax_code_schedule_dispatcher_{instance}`` run per customer.

DAG chain (3-tier):
    main  (this file)   — scheduled poller, 3 tasks
      └─ dispatcher_dag — per-customer entry point; triggers processor,
                          waits, gathers errors, posts run result
           └─ processor_dag — runs the Xero → VP tax code sync pipeline
                              unconditionally (no populate-once skip gates).
                              Reuses the same engine from
                              mapping_sync.utils._tax_code_sync as the
                              mapping_sync map_tax_code DAG, but omits the
                              check_step_complete / is_table_populated gates
                              so the sync executes on every hourly tick.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
from datetime import timedelta
from airflow.models import Variable
import rail
from vp_xero_integration.xero_to_vp_tax_code_schedule import config as sync_config


def create_dag(config):
    """Create hourly main DAG for one instance."""
    with rail.create_airflow_dag(
        dag_id=f'{sync_config.main_dag_id_prefix}_{config.instance}',
        description=sync_config.main_dag_description,
        integration_type=sync_config.integration_type,
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=Variable.get(
            f'{sync_config.schedule_interval_variable_key_prefix}_{config.instance}',
            default_var=sync_config.default_schedule_interval,
        ),
        max_active_runs=config.max_active_runs,
        tags=sync_config.main_dag_tags,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        get_middleware_auth_token = rail.SimpleHttpOperator(
            task_id='get_middleware_auth_token',
            method='POST',
            http_conn_id=config.middleware_conn_id,
            endpoint=sync_config.middleware_auth_endpoint,
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            data={
                'grant_type': 'client_credentials',
                'client_id': (
                    "{{ var.value.get('vantagepoint_client_id', '') }}"
                ),
                'client_secret': (
                    "{{ var.value.get('vantagepoint_client_secret', '') }}"
                ),
            },
            response_filter=lambda response: response.json().get('access_token'),
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
                ),
            },
            data={
                'dag_id': f'{sync_config.dispatcher_dag_id_prefix}_{config.instance}',
                'integration_type': sync_config.middleware_integration_type,
                'status': 'enabled',
            },
            response_filter=lambda response: (
                response.json().get('integrations', [])
            ),
        )

        trigger_tax_code_sync_per_customer = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_tax_code_sync_per_customer',
            items=lambda: rail.result('fetch_customers_by_integration'),
            trigger_dag_id=(
                f'{sync_config.dispatcher_dag_id_prefix}'
                f'_{config.instance}'
            ),
            conf=lambda item: {
                **item,
                'clientId': Variable.get(
                    sync_config.vantagepoint_client_id_variable_key,
                    default_var='',
                ),
                'customerId': item.get('customer_id'),
                'integrationType': 'mapping_sync',
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        (
            get_middleware_auth_token
            >> fetch_customers_by_integration
            >> trigger_tax_code_sync_per_customer
        )

        return dag


rail.for_each_instance(create_dag)

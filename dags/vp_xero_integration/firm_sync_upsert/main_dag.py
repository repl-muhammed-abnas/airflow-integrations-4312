"""
Main DAG for VP -> Xero Firm Sync Upsert.

Scheduled cron entry point. Fetches enabled tenants from the middleware and
triggers the dispatcher DAG per tenant. Mirrors chart_of_accounts_sync/main_dag.py
and QBO customer_sync_upsert/main_dag.py, targeting the VP-source / Xero-sink
direction for VP firms (contacts in Xero).

Porting Workato: 014_501_psa_vantagepoint_firm_upserted (trigger) +
014_501_psa_sync_firms (VP->Xero branch) + 014_501_psa_upsert_contact_in_xero.
"""
from datetime import timedelta
from airflow.models import Variable
import rail


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """Create main scheduler DAG for VP -> Xero Firm Sync Upsert."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_firm_sync_upsert_main_{config.instance}',
        description='Scheduler DAG for VP -> Xero Firm Sync Upsert',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=Variable.get(
            f'vp_xero_firm_sync_upsert_schedule_interval_{config.instance}',
            default_var='*/5 * * * *',
        ),
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_xero', 'firm_sync_upsert', 'main'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
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
            response_filter=lambda response: response.json().get('access_token'),
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
                'dag_id': f'vp_xero_firm_sync_upsert_dispatcher_{config.instance}',
                'integration_type': 'firm_sync_upsert',
                'status': 'enabled',
            },
            response_filter=lambda response: response.json().get('integrations', []),
        )

        _REQUIRED_CONN_KEYS = ('vantagepoint', 'xero')

        def _valid_integration_items():
            """Filter middleware payload to items with both required connections.

            Bad items are logged and skipped rather than raised — a single
            misconfigured tenant must not block fanout to all other tenants.
            """
            items = rail.result('fetch_customers_by_integration') or []
            valid = []
            skipped = []
            for item in items:
                if not isinstance(item, dict):
                    skipped.append(('non-dict', item))
                    continue
                conns = item.get('connections') or {}
                missing = [k for k in _REQUIRED_CONN_KEYS if not conns.get(k)]
                if missing:
                    skipped.append((
                        item.get('customer_id') or '<unknown>',
                        f"missing connections: {missing}",
                    ))
                    continue
                valid.append(item)
            if skipped:
                print(
                    f"Skipped {len(skipped)} middleware integration items "
                    f"with bad shape: {skipped}"
                )
            print(f"Forwarding {len(valid)} tenants to dispatcher fanout")
            return valid

        filter_integrations = rail.PythonOperator(
            task_id='filter_integrations',
            python_callable=_valid_integration_items,
        )

        def _build_dispatcher_conf(item):
            conns = item['connections']
            return {
                **item,
                'clientId': Variable.get('vantagepoint_client_id', default_var=''),
                'customerId': item.get('customer_id'),
                'integrationType': item.get('integration_type'),
                'connections': {
                    'vantagepoint': conns['vantagepoint'],
                    'xero': conns['xero'],
                },
            }

        process_customers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_customers',
            items=lambda: rail.result('filter_integrations'),
            trigger_dag_id=f'vp_xero_firm_sync_upsert_dispatcher_{config.instance}',
            conf=_build_dispatcher_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        (
            get_middleware_auth_token >>
            fetch_customers_by_integration >>
            filter_integrations >>
            process_customers
        )

        return dag


rail.for_each_instance(create_dag)

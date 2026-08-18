"""
Main (scheduler) DAG for VP -> QBO Customer Upsert.

Runs on cron, fetches enabled tenants from the integrations-platform-api
middleware, and triggers one dispatcher DAG run per tenant.
Mirrors `vp_quickbooks_integration/vendor_sync/main_dag.py`.

OPERATIONAL CONSTRAINT — do not register BOTH `customer_upsert`
(this module, VP->QBO) and `customer_sync` (the reverse direction,
QBO->VP) integrations for the SAME customer_id. They will flap forever
as each sync's write triggers the other's poll. Pick the authoritative
direction per tenant.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
from airflow.models import Variable
import rail


def create_dag(config):
    """Top-level scheduler: middleware tenant fetch -> per-tenant fanout."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_customer_upsert_main_{config.instance}',
        description='Scheduler DAG for VP -> QBO Customer Upsert',
        # NOTE: `integration_type='generic'` here is the RAIL framework's
        # DAG classification. It is unrelated to the middleware's own
        # `integration_type=customer_upsert` field used below to filter
        # the /api/v1/integrations response. Same field name, different
        # systems — do not refactor one into the other.
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        # Per-instance schedule override via Airflow Variable, matching the
        # vendor_sync convention. Default cron runs every 15 minutes.
        schedule_interval=Variable.get(
            f'vp_qbo_customer_upsert_schedule_interval_{config.instance}',
            '*/15 * * * *'
        ),
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_quickbooks', 'customer_upsert', 'main'],
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
                    f'vp_qbo_customer_upsert_dispatcher_{config.instance}'
                ),
                'integration_type': 'customer_upsert',
                'status': 'enabled'
            },
            response_filter=lambda response: response.json()['integrations']
        )

        _REQUIRED_CONN_KEYS = (
            'vantagepoint', 'intuit'
        )

        def _valid_integration_items():
            """Filter middleware payload to items with both required connections.

            Bad items are LOGGED and SKIPPED rather than raised — a single
            misconfigured tenant must not block fanout to all other
            tenants in this run. The skipped count is printed so it's
            visible in scheduler logs and a follow-up alert can be set
            up against the pattern.
            """
            items = rail.result('fetch_customers_by_integration') or []
            valid = []
            skipped = []
            for item in items:
                if not isinstance(item, dict):
                    skipped.append(('non-dict', item))
                    continue
                conns = item.get('connections') or {}
                missing = [
                    k for k in _REQUIRED_CONN_KEYS if not conns.get(k)
                ]
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
            python_callable=_valid_integration_items
        )

        def _build_dispatcher_conf(item):
            """Build conf for one (already-validated) dispatcher run.

            Items reaching this point are guaranteed to have both required
            connections present (filter_integrations dropped any bad rows).
            """
            conns = item['connections']
            return {
                **item,
                'clientId': Variable.get(
                    'vantagepoint_client_id', default_var=''
                ),
                'customerId': item.get('customer_id'),
                'integrationType': item.get('integration_type'),
                'connections': {
                    'vantagepoint': conns['vantagepoint'],
                    'intuit': conns['intuit'],
                },
            }

        process_customers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_customers',
            items=lambda: rail.result('filter_integrations'),
            trigger_dag_id=(
                f'vp_qbo_customer_upsert_dispatcher_'
                f'{config.instance}'
            ),
            conf=_build_dispatcher_conf,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        (
            get_middleware_auth_token >>
            fetch_customers_by_integration >>
            filter_integrations >>
            process_customers
        )

        return dag


rail.for_each_instance(create_dag)

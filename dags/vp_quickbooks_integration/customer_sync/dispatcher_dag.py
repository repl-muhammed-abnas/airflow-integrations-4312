"""
Dispatcher DAG for QBO -> VP Customer Sync.

Per-tenant: applies a single polling watermark, runs ONE QBO Customer
query filtered by MetaData.LastUpdatedTime (captures both new AND updated
records in a single stream — QBO bumps LastUpdatedTime on create), then
triggers the router DAG for each record. Mirrors vendor_sync exactly.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_quickbooks_integration.customer_sync.utils.python_callable_method import (  # noqa: E501
    prepare_customer_sync_timestamps_method,
    update_customer_last_sync_times_method,
    is_integration_enabled_method,
    extract_customer_list_method,
)


logger = logging.getLogger(__name__)


def create_dag(config):
    """Per-tenant dispatcher: dual-poll, dedupe, fan out, gather, advance."""
    with rail.create_airflow_dag(
        dag_id=(
            f'vp_qbo_customer_sync_dispatcher_{config.instance}'
        ),
        description=(
            'Poll QBO for new + updated customers and trigger per-record '
            'router DAG'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        # Backstop for stuck deferred sensors so a hung child fanout
        # doesn't pin this DAG's only slot forever.
        dagrun_timeout=timedelta(hours=2),
        tags=['vantagepoint_quickbooks', 'customer_sync', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
        }
    ) as dag:

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=lambda: (
                prepare_customer_sync_timestamps_method(config.instance)
            )
        )

        # Workato CFG_DisableCustomerIntegration account property.
        check_disabled_flag = rail.IfOperator(
            task_id='check_disabled_flag',
            test=lambda: is_integration_enabled_method(config.instance),
            yes_task='get_recently_changed_customers',
            no_task='skip_run'
        )

        skip_run = rail.EmptyOperator(task_id='skip_run')

        # Single QBO poll filtered by `LastUpdatedTime` captures both new
        # and updated customers (QBO bumps LastUpdatedTime on create).
        # Inline-Jinja query mirrors vendor_sync.
        get_recently_changed_customers = rail.QuickBooksCustomerOperator(
            task_id='get_recently_changed_customers',
            intuit_conn_id="{{ dag_run.conf.connections.intuit }}",
            operation='search',
            query=(
                "SELECT * FROM Customer WHERE MetaData.LastUpdatedTime >= "
                "'{{ result('prepare_sync_timestamps')"
                "['last_sync_time'] }}'"
                " AND MetaData.LastUpdatedTime < "
                "'{{ result('prepare_sync_timestamps')"
                "['current_sync_time'] }}'"
            )
        )

        # Flatten the QBO response into the shape downstream DAGs expect
        # (mirrors vendor_sync's extract_vendor_list).
        extract_customers = rail.PythonOperator(
            task_id='extract_customer_list',
            python_callable=extract_customer_list_method
        )

        check_if_records_exist = rail.IfOperator(
            task_id='check_if_records_exist',
            test=lambda: len(rail.result('extract_customer_list')) > 0,
            yes_task='process_records',
            no_task='log_no_records'
        )

        log_no_records = rail.PythonOperator(
            task_id='log_no_records',
            python_callable=lambda: logger.info(
                'No new/updated QBO customer records in this poll window.'
            )
        )

        # Fan out to the router (vendor_sync's pattern). The router
        # decides create vs update by firm-map lookup and triggers the
        # appropriate leaf. Conf is FLATTENED — every record field is
        # promoted to a top-level key (no `record` wrapper), matching
        # vendor_sync's shape.
        process_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_records',
            items=lambda: rail.result('extract_customer_list'),
            trigger_dag_id=(
                f'vp_qbo_customer_sync_router_{config.instance}'
            ),
            conf=lambda item: {
                **item,
                'connections': (
                    rail.get_current_context()['dag_run'].conf
                    .get('connections')
                ),
                'customerId': (
                    rail.get_current_context()['dag_run'].conf
                    .get('customerId')
                ),
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        # Sensor waits for every router run regardless of outcome
        # (failed_states=[]). Per-record errors are still surfaced via
        # gather_router_dag_errors → has_sync_errors → fail_customer_sync,
        # which marks THIS dispatcher run as failed in the UI and posts
        # the details to middleware. We deliberately don't advance the
        # watermark on the failure branch (update_sync_time has default
        # trigger_rule='all_success'), so the next run retries the
        # window — mirrors vendor_sync's conservative "don't lose data"
        # design.
        wait_for_router_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_router_dag_runs',
            dag_runs="{{ result('process_records') }}",
            allowed_states=['success', 'failed'],
            failed_states=[],
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        gather_router_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_router_dag_errors',
            dag_runs="{{ result('process_records') }}",
            dagrun_task_id='catch_router_dag_error',
            flatten=True
        )

        # Callable test (not Jinja string) — RAIL's IfOperator expects a
        # bool-returning callable; a non-empty Jinja string is always
        # truthy and would route every run down the failure branch.
        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=lambda: len(
                rail.result('gather_router_dag_errors') or []
            ) > 0,
            yes_task='fail_customer_sync',
            no_task='update_last_sync_time'
        )

        # FailOperator concatenates per-record errors into a single
        # message and marks the dispatcher run as failed. Middleware
        # picks this up via post_dag_run_details (trigger_rule='all_done').
        fail_customer_sync = rail.FailOperator(
            task_id='fail_customer_sync',
            message=(
                "{{ result('gather_router_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            )
        )

        # trigger_rule='all_done' so the watermark advances even when the
        # error branch's FailOperator raises. Without it, the default
        # all_success rule skips this task and the next poll re-processes
        # the same window forever.
        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            python_callable=lambda: (
                update_customer_last_sync_times_method(config.instance)
            ),
            trigger_rule='all_done'
        )

        # Use a Jinja `{{ var.value.middleware_api_base_url }}` lookup
        # instead of `Variable.get(...)` so the metadata-DB read happens at
        # task execution rather than DAG parse time — DAG parsing then
        # works in CI / fresh deploys before the Variable is populated.
        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=(
                "{{ var.value.get('middleware_api_base_url', '') }}"
            ),
            trigger_rule='all_done'
        )

        prepare_timestamps >> check_disabled_flag
        check_disabled_flag >> rail.Label('Disabled') >> skip_run
        skip_run >> update_sync_time

        # Enabled path: single QBO poll -> extract -> branch on count.
        (
            check_disabled_flag >> rail.Label('Enabled') >>
            get_recently_changed_customers >> extract_customers >>
            check_if_records_exist
        )

        (
            check_if_records_exist >> rail.Label('No records') >>
            log_no_records >> update_sync_time
        )

        (
            check_if_records_exist >> rail.Label('Records found') >>
            process_records >> wait_for_router_dag_runs >>
            gather_router_dag_errors >> has_sync_errors
        )

        has_sync_errors >> rail.Label('No') >> update_sync_time
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_customer_sync >> update_sync_time
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)

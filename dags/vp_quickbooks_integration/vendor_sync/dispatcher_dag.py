"""
Dispatcher DAG for VP QBO Vendor Sync.
Fetches recently changed vendors from QuickBooks and triggers sync.
"""
import logging
from datetime import timedelta
from airflow.models import Variable
import rail
from vp_quickbooks_integration.common.python_callable_method import (
    prepare_sync_timestamps,
    update_last_sync_time,
)
from vp_quickbooks_integration.vendor_sync.config import (
    initial_sync_time,
    watermark_variable_key_template,
)

logger = logging.getLogger(__name__)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,too-many-locals
def create_dag(config):
    """
    Create dispatcher DAG for fetching recently changed vendors.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_vendor_sync_dispatcher_{config.instance}',
        description=(
            'Fetch recently changed vendors from QuickBooks and trigger sync'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_quickbooks', 'vendor_sync', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=lambda: prepare_sync_timestamps(
                config.instance,
                watermark_variable_key_template,
                initial_sync_time,
            )
        )

        get_recently_changed_vendors = rail.QuickBooksVendorOperator(
            task_id='get_recently_changed_vendors',
            intuit_conn_id=(
                "{{ dag_run.conf.connections.intuit }}"
            ),
            operation='search',
            query=(
                "SELECT * FROM Vendor WHERE MetaData.LastUpdatedTime >= "
                "'{{ result('prepare_sync_timestamps')"
                "['last_sync_time'] }}'"
                " AND MetaData.LastUpdatedTime < "
                "'{{ result('prepare_sync_timestamps')"
                "['current_sync_time'] }}'"
            )
        )

        def extract_vendor_list():
            """Extract vendor list from the QuickBooksVendorOperator response dict."""
            result = rail.result('get_recently_changed_vendors')
            if not result.get('success'):
                logger.error("QuickBooks vendor query failed: %s",
                             result.get('error'))
                return []
            vendors = result.get('data') or []
            logger.info("Found %d recently changed vendors", len(vendors))
            return vendors

        extract_vendors = rail.PythonOperator(
            task_id='extract_vendor_list',
            python_callable=extract_vendor_list
        )

        check_if_vendors_exist = rail.IfOperator(
            task_id='check_if_vendors_exist',
            test=lambda: len(rail.result('extract_vendor_list')) > 0,
            yes_task='process_vendors',
            no_task='log_no_vendors'
        )

        def log_no_vendors_found():
            timestamps = rail.result('prepare_sync_timestamps')
            logger.info(
                "No recently changed vendors found in QuickBooks "
                "(query range: %s to %s)",
                timestamps['last_sync_time'], timestamps['current_sync_time']
            )

        log_no_vendors = rail.PythonOperator(
            task_id='log_no_vendors',
            python_callable=log_no_vendors_found
        )

        process_vendors = rail.TriggerDagRunForEachItemOperator(
            task_id='process_vendors',
            items=lambda: rail.result('extract_vendor_list'),
            trigger_dag_id=f'vp_qbo_vendor_sync_router_{config.instance}',
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
                # Forward the middleware integration `config` block (CFG_* values
                # like CFG_DefaultVendorType) so the create DAG can resolve
                # per-tenant defaults from conf['config'].
                'config': (
                    rail.get_current_context()['dag_run'].conf
                    .get('config')
                )
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_router_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_router_dag_runs',
            dag_runs="{{ result('process_vendors') }}",
            allowed_states=['success', 'failed'],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_router_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_router_dag_errors',
            dag_runs="{{ result('process_vendors') }}",
            dagrun_task_id='catch_router_dag_error',
            flatten=True
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test="{{ result('gather_router_dag_errors') | length > 0 }}",
            yes_task='fail_vendor_sync',
            no_task='update_last_sync_time'
        )

        fail_vendor_sync = rail.FailOperator(
            task_id='fail_vendor_sync',
            message=(
                "{{ result('gather_router_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            )
        )

        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            python_callable=lambda: update_last_sync_time(
                config.instance,
                watermark_variable_key_template,
            )
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=Variable.get(
                'middleware_api_base_url', default_var=''
            ),
            trigger_rule='all_done'
        )

        (
            prepare_timestamps >> get_recently_changed_vendors >>
            extract_vendors >> check_if_vendors_exist
        )

        (
            check_if_vendors_exist >> rail.Label('Vendors found') >>
            process_vendors >> wait_for_router_dag_runs >>
            gather_router_dag_errors >> has_sync_errors
        )
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_vendor_sync >> update_sync_time
        )
        has_sync_errors >> rail.Label('No') >> update_sync_time

        (
            check_if_vendors_exist >> rail.Label('No vendors') >>
            log_no_vendors >> update_sync_time
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)

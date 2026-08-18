"""
Dispatcher DAG for VP PSA -> Xero Posted Invoices Sync.

Per-tenant: applies the polling watermark, queries the VP PSA Ledger for
newly posted AR invoice records (TransType='IN'), extracts unique batch
numbers, and triggers one processor DAG run per batch. After all processor
runs complete, gathers errors and advances the watermark.

Replaces the Workato `014-501 PSA Poll Vantagepoint Posted Invoices for Xero`
trigger recipe's per-batch fan-out stage.
"""
# pylint: disable=too-many-statements,line-too-long,pointless-statement
# pylint: disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_xero_integration.vp_to_xero_posted_invoice_sync.config import (
    initial_sync_time,
    watermark_variable_key_template,
)
from vp_xero_integration.common.python_callable_method import (
    prepare_sync_timestamps,
    update_last_sync_time,
    has_sync_errors_method,
)
from vp_xero_integration.vp_to_xero_posted_invoice_sync.utils.python_callable_method import (
    build_psa_ledger_filter_method,
    extract_invoice_batches_method,
    build_processor_dag_conf,
)

logger = logging.getLogger(__name__)


def create_dag(config):
    """Per-tenant dispatcher: poll PSA Ledger, extract batches, fan out, gather, advance watermark."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_posted_invoice_sync_dispatcher_{config.instance}',
        description='Poll VP PSA AR invoice ledger and trigger per-batch Xero posted-invoices processor',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_xero', 'posted_invoices', 'dispatcher'],
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

        # Replaces Workato `sales_invoice_created` trigger.
        # TransType='IN' matches posted AR invoice ledger rows.
        poll_psa_ledger = rail.VantagepointPsaledgerOperator(
            task_id='poll_psa_ledger',
            vp_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default') }}"
            ),
            filters=build_psa_ledger_filter_method,
            trans_type='IN',
        )

        extract_batches = rail.PythonOperator(
            task_id='extract_batches',
            python_callable=extract_invoice_batches_method,
        )

        check_if_batches_exist = rail.IfOperator(
            task_id='check_if_batches_exist',
            test=lambda: len(rail.result('extract_batches') or []) > 0,
            yes_task='process_batches',
            no_task='log_no_batches',
        )

        log_no_batches = rail.PythonOperator(
            task_id='log_no_batches',
            python_callable=lambda: logger.info(
                'No new posted AR invoice batches in this poll window (%s to %s)',
                (rail.result('prepare_sync_timestamps') or {}).get('last_sync_time', ''),
                (rail.result('prepare_sync_timestamps') or {}).get('current_sync_time', ''),
            )
        )

        process_batches = rail.TriggerDagRunForEachItemOperator(
            task_id='process_batches',
            items=lambda: rail.result('extract_batches'),
            trigger_dag_id=f'vp_xero_posted_invoice_sync_processor_{config.instance}',
            conf=build_processor_dag_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Cover every terminal child-dag state so the sensor never stalls.
        # `failed_states=[]` keeps it from short-circuiting on the first failure
        # — we gather errors from all children before deciding whether to advance
        # the watermark.
        wait_for_processor_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_processor_dag_runs',
            dag_runs="{{ result('process_batches') }}",
            allowed_states=['success', 'failed', 'upstream_failed', 'removed'],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_processor_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_processor_dag_errors',
            dag_runs="{{ result('process_batches') }}",
            dagrun_task_id='catch_processor_dag_error',
            flatten=True,
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=has_sync_errors_method,
            yes_task='fail_posted_invoices_sync',
            no_task='update_last_sync_time',
        )

        fail_posted_invoices_sync = rail.FailOperator(
            task_id='fail_posted_invoices_sync',
            message=(
                "{{ result('gather_processor_dag_errors')"
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
            middleware_api_base_url="{{ var.value.get('middleware_api_base_url', '') }}",
            trigger_rule='all_done',
        )

        # ------------------------------------------------------------------ #
        # Task graph
        # ------------------------------------------------------------------ #
        (prepare_timestamps >> poll_psa_ledger >> extract_batches >> check_if_batches_exist)

        (
            check_if_batches_exist >> rail.Label('No batches') >>
            log_no_batches >> update_sync_time
        )

        (
            check_if_batches_exist >> rail.Label('Batches found') >>
            process_batches >> wait_for_processor_dag_runs >>
            gather_processor_dag_errors >> has_sync_errors
        )

        has_sync_errors >> rail.Label('No') >> update_sync_time
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_posted_invoices_sync >> post_dag_run_details
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)

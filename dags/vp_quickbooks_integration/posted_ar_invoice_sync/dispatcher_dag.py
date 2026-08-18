"""
Dispatcher DAG for VP PSA -> QBO Posted AR Invoice Sync.

Per-tenant: applies the polling watermark, queries the VP PSA Ledger for
newly posted AR invoice records (TransType='IN'), extracts unique batch
numbers, and triggers one router DAG run per batch. The router DAG reads
CFG_Region and forwards to the appropriate regional create DAG (US or CA/UK).
After all router runs complete, gathers errors and advances the watermark.

Replaces the Workato `014-503 PSA Poll Vantagepoint Posted AR Invoice Flow`
routing stage, mapping:
  - Workato polling trigger → VantagepointPsaledgerOperator (trans_type='IN')
  - Batch fan-out           → TriggerDagRunForEachItemOperator
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
from datetime import timedelta
import rail
from vp_quickbooks_integration.posted_ar_invoice_sync.utils.python_callable_method import (
    prepare_sync_timestamps_method,
    update_last_sync_time_method,
    build_psa_ledger_filter_method,
    extract_invoice_batches_method,
)


def create_dag(config):
    """Per-tenant dispatcher: poll PSA Ledger, extract batches, fan out, gather, advance watermark."""
    with rail.create_airflow_dag(
        dag_id=f'vp_qbo_ar_invoice_sync_dispatcher_{config.instance}',
        description=(
            'Poll VP PSA AR invoice ledger and trigger per-batch create DAG'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        dagrun_timeout=timedelta(hours=2),
        tags=['vantagepoint_quickbooks', 'ar_invoice_sync', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days
            )
        }
    ) as dag:

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=lambda: prepare_sync_timestamps_method(
                config.instance
            )
        )

        # Replaces Workato `sales_invoice_created` trigger.
        # TransType='IN' matches posted AR invoice ledger rows.
        # skipActivePeriod / skipActiveCompany are encoded in the OData
        # filter via PostDate range; the operator handles VP auth and pagination.
        poll_psa_ledger = rail.VantagepointPsaledgerOperator(
            task_id='poll_psa_ledger',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            filters=build_psa_ledger_filter_method,
            trans_type='IN'
        )

        # Deduplicate to one item per unique Batch number.
        extract_batches = rail.PythonOperator(
            task_id='extract_batches',
            python_callable=extract_invoice_batches_method
        )

        check_if_batches_exist = rail.IfOperator(
            task_id='check_if_batches_exist',
            test=lambda: len(rail.result('extract_batches') or []) > 0,
            yes_task='process_batches',
            no_task='log_no_batches'
        )

        log_no_batches = rail.PythonOperator(
            task_id='log_no_batches',
            python_callable=lambda: print(
                'No new posted AR invoice batches in this poll window.'
            )
        )

        def build_create_dag_conf(item):
            conf = rail.get_current_context()['dag_run'].conf
            return {
                'Batch': item.get('Batch'),
                'PostDate': item.get('PostDate'),
                'connections': conf.get('connections'),
                'customerId': conf.get('customerId'),
                'config': conf.get('config', {}),
            }

        process_batches = rail.TriggerDagRunForEachItemOperator(
            task_id='process_batches',
            items=lambda: rail.result('extract_batches'),
            trigger_dag_id=(
                f'vp_qbo_ar_invoice_sync_router_{config.instance}'
            ),
            conf=build_create_dag_conf,
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        wait_for_create_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_dag_runs',
            dag_runs="{{ result('process_batches') }}",
            allowed_states=[
                'success', 'failed', 'upstream_failed', 'removed'
            ],
            failed_states=[],
            execution_timeout=timedelta(
                days=config.execution_timeout_days
            )
        )

        gather_create_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_create_dag_errors',
            dag_runs="{{ result('process_batches') }}",
            dagrun_task_id='catch_router_dag_error',
            flatten=True
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=lambda: len(
                rail.result('gather_create_dag_errors') or []
            ) > 0,
            yes_task='fail_ar_invoice_sync',
            no_task='update_last_sync_time'
        )

        fail_ar_invoice_sync = rail.FailOperator(
            task_id='fail_ar_invoice_sync',
            message=(
                "{{ result('gather_create_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            )
        )

        # trigger_rule='all_done' so the watermark advances even when the
        # error branch fires — prevents the same window being re-processed
        # on the next poll after a partial-batch failure.
        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            trigger_rule='all_done',
            python_callable=lambda: update_last_sync_time_method(
                config.instance
            )
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=(
                "{{ var.value.get('middleware_api_base_url', '') }}"
            ),
            trigger_rule='all_done'
        )

        # ------------------------------------------------------------------ #
        # Task graph
        # ------------------------------------------------------------------ #
        prepare_timestamps >> poll_psa_ledger
        poll_psa_ledger >> extract_batches >> check_if_batches_exist

        (
            check_if_batches_exist >> rail.Label('No batches') >>
            log_no_batches >> update_sync_time
        )

        (
            check_if_batches_exist >> rail.Label('Batches found') >>
            process_batches >> wait_for_create_dag_runs >>
            gather_create_dag_errors >> has_sync_errors
        )

        has_sync_errors >> rail.Label('No') >> update_sync_time
        (
            has_sync_errors >> rail.Label('Yes') >>
            fail_ar_invoice_sync >> post_dag_run_details
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)

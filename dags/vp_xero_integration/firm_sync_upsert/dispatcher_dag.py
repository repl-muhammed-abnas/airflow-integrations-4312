"""
Dispatcher DAG for VP -> Xero Firm Sync Upsert.

Per-tenant: applies the polling watermark, asks VP for firms modified since
the last run (filterHash datetime window), guards against in-progress
mapping_sync, and triggers the processor DAG per firm.

Porting Workato:
  014_501_psa_vantagepoint_firm_upserted  (trigger + initial-sync guard)
  014_501_psa_sync_firms                  (callable orchestration, VP->Xero branch)

Polling: VantagepointFirmOperator GET /firm with filterHash [ModDate >= last,
ModDate < current] — no ClientInd filter (Xero syncs BOTH client and vendor
firms, unlike QBO which restricts to ClientInd=Y).

Topology mirrors QBO customer_sync_upsert/dispatcher_dag.py:
  check_initial_sync_complete → is_mapping_ready
    → Yes: prepare_timestamps → get_recently_changed_firms → extract_firms
           → check_if_firms_exist
             → Firms found: process_firms → wait → gather → has_sync_errors
                                                              → fail / advance
             → No firms: log_no_firms → advance
    → No: skip_mapping_not_ready → (watermark unchanged via all_done guard)
  update_last_sync_time (trigger_rule='all_done') → post_dag_run_details
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,import-error
import logging
from datetime import timedelta
import rail
from vp_xero_integration.firm_sync_upsert.config import (
    initial_sync_time,
    watermark_variable_key_template,
)
from vp_xero_integration.common.python_callable_method import (
    prepare_sync_timestamps,
    update_last_sync_time,
)
from vp_xero_integration.firm_sync_upsert.utils.python_callable_method import (
    build_firm_poll_filter,
    extract_firm_list_method,
    check_firms_exist_method,
    check_initial_sync_complete_method,
    build_process_firm_conf,
    has_sync_errors,
)

logger = logging.getLogger(__name__)


def create_dag(config):
    """Per-tenant dispatcher: poll VP firms, fan out, gather, advance watermark."""
    with rail.create_airflow_dag(
        dag_id=f'vp_xero_firm_sync_upsert_dispatcher_{config.instance}',
        description=(
            'Poll VP for recently updated firms and trigger per-firm '
            'Xero contact upsert processor DAG'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        dagrun_timeout=timedelta(hours=2),
        tags=['vantagepoint_xero', 'firm_sync_upsert', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        # ---- Initial-sync concurrency guard ----
        # Workato step 1: stop if 014_501_psa_synch_firms has pending jobs.
        # In Airflow: skip if mapping_sync Map Firms step is not yet Complete.
        check_initial_sync_complete = rail.PythonOperator(
            task_id='check_initial_sync_complete',
            python_callable=check_initial_sync_complete_method,
        )

        is_mapping_ready = rail.IfOperator(
            task_id='is_mapping_ready',
            test=lambda: rail.result('check_initial_sync_complete'),
            yes_task='prepare_sync_timestamps',
            no_task='skip_mapping_not_ready',
        )

        skip_mapping_not_ready = rail.PythonOperator(
            task_id='skip_mapping_not_ready',
            python_callable=lambda: logger.info(
                "Skipping firm_sync_upsert run: mapping_sync Map Firms step "
                "is not yet Complete for this customer"
            ),
        )

        # ---- Watermark ----
        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=lambda: prepare_sync_timestamps(
                config.instance,
                watermark_variable_key_template,
                initial_sync_time,
            ),
        )

        # ---- VP firm poll ----
        # filterHash: ModDate >= last_sync AND ModDate < current_sync.
        # `filters` is NOT a template_field on VantagepointFirmOperator —
        # must be a callable that reads XCom via context['ti'].xcom_pull.
        get_recently_changed_firms = rail.VantagepointFirmOperator(
            task_id='get_recently_changed_firms',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            request_method='GET',
            filters=build_firm_poll_filter,
        )

        extract_firms = rail.PythonOperator(
            task_id='extract_firm_list',
            python_callable=extract_firm_list_method,
        )

        check_if_firms_exist = rail.IfOperator(
            task_id='check_if_firms_exist',
            test=check_firms_exist_method,
            yes_task='process_firms',
            no_task='log_no_firms',
        )

        log_no_firms = rail.PythonOperator(
            task_id='log_no_firms',
            python_callable=lambda: logger.info(
                "No recently updated VP firms found in poll window (%s to %s)",
                rail.result('prepare_sync_timestamps')['last_sync_time'],
                rail.result('prepare_sync_timestamps')['current_sync_time'],
            ),
        )

        # ---- Fan-out ----
        process_firms = rail.TriggerDagRunForEachItemOperator(
            task_id='process_firms',
            items=lambda: rail.result('extract_firm_list'),
            trigger_dag_id=f'vp_xero_firm_sync_upsert_processor_{config.instance}',
            conf=build_process_firm_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Cover every terminal child-dag state so the sensor never stalls.
        # `failed_states=[]` means we gather ALL results before deciding;
        # a failed child DAG is captured in gather_processor_dag_errors,
        # NOT treated as a hard sensor failure.
        wait_for_processor_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_processor_dag_runs',
            dag_runs="{{ result('process_firms') }}",
            allowed_states=['success', 'failed', 'upstream_failed', 'removed'],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_processor_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_processor_dag_errors',
            dag_runs="{{ result('process_firms') }}",
            dagrun_task_id='catch_processor_dag_error',
            flatten=True,
        )

        has_sync_errors_gate = rail.IfOperator(
            task_id='has_sync_errors',
            test=has_sync_errors,
            yes_task='fail_firm_sync',
            no_task='update_last_sync_time',
        )

        fail_firm_sync = rail.FailOperator(
            task_id='fail_firm_sync',
            message=(
                "{{ result('gather_processor_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            ),
        )

        # trigger_rule='all_done': watermark always advances once a run reaches
        # a terminal state — including after partial failures already captured in
        # gather_processor_dag_errors, and after the skip_mapping_not_ready branch
        # (where prepare_sync_timestamps was skipped; update_last_sync_time handles
        # the None result gracefully by leaving the Variable unchanged).
        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            trigger_rule='all_done',
            python_callable=lambda: update_last_sync_time(
                config.instance,
                watermark_variable_key_template,
            ),
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=(
                "{{ var.value.get('middleware_api_base_url', '') }}"
            ),
            trigger_rule='all_done',
        )

        # ---- Wiring ----
        check_initial_sync_complete >> is_mapping_ready

        # Not-ready path: skip, connect to update_sync_time so all_done fires
        # but watermark stays unchanged (prepare_sync_timestamps was skipped).
        is_mapping_ready >> rail.Label('Mapping not ready') >> skip_mapping_not_ready >> update_sync_time

        # Ready path: full polling + fan-out chain.
        is_mapping_ready >> rail.Label('Mapping ready') >> prepare_timestamps
        (
            prepare_timestamps >>
            get_recently_changed_firms >>
            extract_firms >>
            check_if_firms_exist
        )

        # No firms in window.
        (
            check_if_firms_exist >> rail.Label('No firms') >>
            log_no_firms >> update_sync_time
        )

        # Firms found: fan-out → gather → fail-or-advance.
        (
            check_if_firms_exist >> rail.Label('Firms found') >>
            process_firms >> wait_for_processor_dag_runs >>
            gather_processor_dag_errors >> has_sync_errors_gate
        )
        has_sync_errors_gate >> rail.Label('No') >> update_sync_time
        (
            has_sync_errors_gate >> rail.Label('Yes') >>
            fail_firm_sync >> update_sync_time
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)

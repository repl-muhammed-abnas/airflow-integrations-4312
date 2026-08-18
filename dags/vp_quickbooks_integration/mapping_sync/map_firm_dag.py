"""
Map Firm child DAG for VP QBO Mapping Sync.

Direction (per Workato recipe `014_503_psa_synch_firms`):
- QBO Customer/Vendor → VP Firm (forward sync; QBO is master)

This DAG mirrors Workato's initial-sync shape, which is forward-only.
The VP→QBO push (per-firm "customer upserted in Vantagepoint") lives
in a separate event-driven recipe in Workato
(`014_503_psa_customer_upserted_in_vantagepoint`) and would be a
separate trigger DAG here if it's ever needed — not part of the
initial mapping sync.

The forward sync is map-only (Workato parity): a QBO entity that already
resolves to a VP firm is updated (PUT) and its row gets that ClientID;
an entity with no VP firm is recorded UNMAPPED (blank FirmID) and is NOT
created in VP. The VP→QBO push (per-firm "customer upserted in
Vantagepoint") lives in a separate event-driven recipe in Workato
(`014_503_psa_customer_upserted_in_vantagepoint`) and would be a separate
trigger DAG here if it's ever needed.

Writes into the `map_firm` S3 collection. The dispatcher creates the
table up front (with a UNIQUE(QBOID, IsVendor) index so re-runs upsert
rather than append); this DAG only populates rows. Columns:
    FirmID   — VP firm ClientID (blank when the QBO entity is unmapped)
    QBOID    — QBO Customer / Vendor Id
    IsVendor — 'Y' / 'N'
    Name

Flow:
    check_map_firm_populated → is_map_firm_populated
       ├─ Yes → skip_populate_map_firm
       └─ No  → fetch_qbo_customers (QuickBooksCustomerOperator search)
              → fetch_qbo_vendors   (QuickBooksVendorOperator search)
              → process_qbo_firms   (PythonOperator: per-record map →
                                     PUT update if mapped, else blank-FirmID
                                     row; map_firm row upsert)
       catch_map_firm_dag_error  (one_failed; returns dict, never raises)
"""
from vp_quickbooks_integration.common.tables import (
    MAPPING_STEP_FIRM,
)
from vp_quickbooks_integration.mapping_sync.utils.python_callable_method import (
    is_table_populated,
    capture_dag_error,
    check_step_status,
    mark_step_status,
    sync_qbo_firms_to_vp,
)
from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig
import logging
from datetime import timedelta
from airflow.models import Variable
import rail

_log = logging.getLogger(__name__)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """Per-instance map_firm child DAG."""
    with rail.create_airflow_dag(
        dag_id=IntegrationConfig.dag_id('map_firm', config.instance),
        description=(
            'Sync QBO customers/vendors to VP firms; reverse-sync net-new '
            'VP clients to QBO. Writes the map_firm cross-reference table.'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        tags=['vantagepoint_quickbooks', 'mapping_sync', 'map_firm'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        # ---- Batch-task gate (perf opt-out) ----
        # When the shared Variable `vp_qbo_mapping_sync_can_run_batch`
        # is 'true' (default), the entire success path runs inside one
        # `BatchTaskRunOperator` — sequential, single-process, no
        # Airflow scheduler context-switching between tasks. Flipping
        # the Variable to 'false' falls back to the legacy per-task
        # scheduling for diagnosis.
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                IntegrationConfig.CAN_RUN_BATCH_VARIABLE_NAME,
                default_var='true',
            ).lower() == 'true',
            yes_task='batch_task',
            no_task='is_map_firm_populated',
        )

        # ---- Skip gates (layered) ----
        # Primary: mapping_table_state.Status == 'Complete' for this
        # step. Set by `apply_premapping_state` when CFG_UpgradeDataSync
        # is false, or by a prior successful run of this DAG.
        # Secondary: is_table_populated as a defensive fallback in case
        # the state table is missing rows (e.g. someone triggers this
        # child DAG directly without running the dispatcher first).
        check_step_complete = rail.PythonOperator(
            task_id='check_map_firm_step_complete',
            python_callable=lambda: check_step_status(MAPPING_STEP_FIRM),
        )

        check_populated = rail.PythonOperator(
            task_id='check_map_firm_populated',
            python_callable=lambda: is_table_populated('map_firm'),
        )

        is_populated = rail.IfOperator(
            task_id='is_map_firm_populated',
            test=lambda: (
                rail.result('check_map_firm_step_complete') or
                rail.result('check_map_firm_populated')
            ),
            yes_task='skip_populate_map_firm',
            no_task='fetch_qbo_customers',
        )

        skip_populate = rail.PythonOperator(
            task_id='skip_populate_map_firm',
            python_callable=lambda: _log.info(
                'map_firm already populated for this customer — skipping'
            ),
        )

        # ---- Mark step Complete on successful population ----
        # Workato parity: synch_mapped_data sets col4='Complete' after
        # each sub-recipe call (recipe lines 250-256).
        mark_step_complete = rail.PythonOperator(
            task_id='mark_map_firm_step_complete',
            python_callable=lambda: mark_step_status(
                MAPPING_STEP_FIRM, 'Complete'
            ),
        )

        # ---- Forward sync (QBO → VP): fetch QBO entities, then per-record
        # VP create/update via the helper. Connection IDs flow through
        # dag_run.conf.connections; defaults fall back to the constants in
        # IntegrationConfig.
        fetch_qbo_customers = rail.QuickBooksCustomerOperator(
            task_id='fetch_qbo_customers',
            intuit_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('intuit', 'quickbooks_default') }}"
            ),
            operation='search',
            query='select * from Customer where Active = true',
        )

        fetch_qbo_vendors = rail.QuickBooksVendorOperator(
            task_id='fetch_qbo_vendors',
            intuit_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('intuit', 'quickbooks_default') }}"
            ),
            operation='search',
            query='select * from Vendor where Active = true',
        )

        process_qbo_firms = rail.PythonOperator(
            task_id='process_qbo_firms',
            python_callable=sync_qbo_firms_to_vp,
            op_args=[config.instance],
        )

        catch_map_firm_dag_error = rail.PythonOperator(
            task_id='catch_map_firm_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_dag_error,
            op_args=[
                'map_firm',
                "{{ dag_run.conf.get('customerId') or '' }}",
                '{{ get_error_message() }}',
            ],
        )

        # Batch wraps the linear chain from is_map_firm_populated
        # through mark_map_firm_step_complete. On a task failure inside
        # the range the operator routes to the downstream
        # `catch_map_firm_dag_error` (trigger_rule='one_failed').
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_map_firm_populated',
            end_task='catch_map_firm_dag_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ---- Wiring ----
        # Pre-batch fan-in: the two check_* PythonOperators run first
        # (they're outside the batch range — their results feed the
        # is_map_firm_populated IfOperator inside the batch via XCom).
        [check_step_complete, check_populated] >> can_run_batch_task

        # Batch path: BatchTaskRunOperator runs is_populated -> chain ->
        # mark_step_complete in-process. Errors inside the range route
        # to catch_map_firm_dag_error via its trigger_rule='one_failed'.
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_map_firm_dag_error

        # Non-batch path: legacy per-task Airflow scheduling. Linear
        # chain ending in mark_step_complete; the trigger_rule on the
        # catch task picks up any upstream failure naturally.
        can_run_batch_task >> rail.Label('No') >> is_populated
        is_populated >> rail.Label(
            'Already populated') >> skip_populate >> catch_map_firm_dag_error
        (
            is_populated >> rail.Label('Needs population') >>
            fetch_qbo_customers >> fetch_qbo_vendors >>
            process_qbo_firms >> mark_step_complete >> catch_map_firm_dag_error
        )

        return dag


rail.for_each_instance(create_dag)

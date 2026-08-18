"""
Map Firm child DAG for VP Xero Mapping Sync.

Direction (per Workato recipe `014_501_psa_synch_firms` + the `Map Firms`
seeder, folded together — see reverse-engineering docs 01 + 06):
- Xero Contact → VP Firm (forward sync; Xero is master)

The engine seeds + syncs in one pass: fetch all Xero contacts (paginated,
active + archived), anti-join against existing map_firm rows (by ContactID),
match each remaining contact to a VP firm by Name (reuse MIN(ClientID)) or
create the VP firm + addresses, then upsert the map_firm cross-reference row.

Writes into the `map_firm` S3 collection (created up front by the dispatcher
with a UNIQUE(ContactID) index so re-runs upsert rather than append). Columns:
    FirmID, ContactID, Status, Vendor, Client, XeroName, VantagepointName, ModDate

Flow:
    check_map_firm_step_complete → is_map_firm_populated
       ├─ Yes (Status='Complete') → skip_populate_map_firm
       └─ No  → fetch_xero_contacts (XeroContactOperator search, paginated,
                                     include_archived)
              → process_xero_firms  (PythonOperator: sync_xero_firms_to_vp)
              → mark_map_firm_step_complete
       catch_map_firm_dag_error  (one_failed; returns dict, never raises)
"""
from vp_xero_integration_v2.common.tables import (
    MAPPING_STEP_FIRM,
)
from vp_xero_integration_v2.mapping_sync.utils.python_callable_method import (
    capture_dag_error,
    check_step_status,
    mark_step_status,
    sync_xero_firms_to_vp,
)
from vp_xero_integration_v2.mapping_sync.config import IntegrationConfig
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
            'Sync Xero contacts to VP firms (match-by-name or create) and '
            'write the map_firm cross-reference table.'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        tags=['vantagepoint_xero', 'mapping_sync', 'map_firm'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        # ---- Batch-task gate (perf opt-out) ----
        # When the shared Variable `vp_xero_mapping_sync_can_run_batch` is 'true'
        # (default), the success path runs inside one `BatchTaskRunOperator` —
        # sequential, single-process. Flipping the Variable to 'false' falls back
        # to legacy per-task scheduling for diagnosis.
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                IntegrationConfig.CAN_RUN_BATCH_VARIABLE_NAME,
                default_var='true',
            ).lower() == 'true',
            yes_task='batch_task',
            no_task='is_map_firm_populated',
        )

        # ---- Skip gate ----
        # mapping_table_state.Status == 'Complete' for this step: set by
        # apply_premapping_state when the table already has data, or by a prior
        # successful run's mark_map_firm_step_complete. S3UpsertCollectionOperator
        # is keyed on ContactID (UNIQUE index) so re-runs are idempotent — the
        # secondary is_table_populated check is not needed and was removed to
        # prevent partial-failure rows from blocking a retry.
        check_step_complete = rail.PythonOperator(
            task_id='check_map_firm_step_complete',
            python_callable=lambda: check_step_status(MAPPING_STEP_FIRM),
        )

        is_populated = rail.IfOperator(
            task_id='is_map_firm_populated',
            test=lambda: rail.result('check_map_firm_step_complete'),
            yes_task='skip_populate_map_firm',
            no_task='fetch_xero_contacts',
        )

        skip_populate = rail.PythonOperator(
            task_id='skip_populate_map_firm',
            python_callable=lambda: _log.info(
                'map_firm already populated for this customer — skipping'
            ),
        )

        # ---- Mark step Complete on successful population ----
        mark_step_complete = rail.PythonOperator(
            task_id='mark_map_firm_step_complete',
            python_callable=lambda: mark_step_status(
                MAPPING_STEP_FIRM, 'Complete'
            ),
        )

        # ---- Forward sync (Xero → VP): fetch all contacts (paginated,
        # active + archived), then per-record match/create via the engine.
        # Connection id flows through dag_run.conf.connections.xero; default
        # falls back to xero_default.
        fetch_xero_contacts = rail.XeroContactOperator(
            task_id='fetch_xero_contacts',
            xero_conn_id=(
                "{{ dag_run.conf.get('connections', {}).get('xero', 'xero_default') }}"
            ),
            operation='search',
            include_archived=True,
            paginate=True,
        )

        process_xero_firms = rail.PythonOperator(
            task_id='process_xero_firms',
            python_callable=sync_xero_firms_to_vp,
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

        # Batch wraps the linear chain from is_map_firm_populated through
        # mark_map_firm_step_complete. On a task failure inside the range the
        # operator routes to catch_map_firm_dag_error (trigger_rule='one_failed').
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_map_firm_populated',
            end_task='catch_map_firm_dag_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ---- Wiring ----
        # check_step_complete runs first (outside the batch range — its result
        # feeds the is_map_firm_populated IfOperator inside the batch via XCom).
        check_step_complete >> can_run_batch_task

        # Batch path.
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_map_firm_dag_error

        # Non-batch path: legacy per-task Airflow scheduling.
        can_run_batch_task >> rail.Label('No') >> is_populated
        is_populated >> rail.Label(
            'Already populated') >> skip_populate >> catch_map_firm_dag_error
        (
            is_populated >> rail.Label('Needs population') >>
            fetch_xero_contacts >> process_xero_firms >>
            mark_step_complete >> catch_map_firm_dag_error
        )

        return dag


rail.for_each_instance(create_dag)

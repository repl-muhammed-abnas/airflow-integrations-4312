"""
Dispatcher DAG for VP Xero Mapping Sync (V2 IPA GitSync architecture).

Per-customer orchestrator. Creates the S3 mapping tables, seeds the
mapping_table_state rows + the map_account_type reference data, applies
per-step content-aware premapping, triggers three mapping child DAGs
(map_firm, map_account_code, map_tax_code) then validate_mappings in priority
order, gathers any child errors, and either fails the run or posts a
successful run-details payload + final 'Ready' state handshake.

Employee mapping is out of scope (Q1 = No employee sync).

V2 changes from V1:
  - schedule_interval from config.schedule_interval (not None)
  - connections/customerId in build_child_dag_conf injected from config
  - middleware_api_base_url via Jinja var.value.get (not parse-time Variable.get)
  - check_disabled_flag / skip_run removed (RAIL handles disabled=True at parse time)
  - no main_dag.py (IPA GitSync architecture)

Sequence (strict Phase-3 order):
  is_mapping_init_already_done  ─ per-customer one-shot init gate.
            │ (Not initialized)
  init_mapping_collections  ─ S3CreateMultiTableCollectionOperator.
            │                  Creates the collection tables, seeds
            │                  mapping_table_state (3 step rows: firm,
            │                  account, tax_code) AND seeds map_account_type
            │                  (Xero account-type → VP type lookup) in one S3
            │                  round-trip.
            ▼
  apply_premapping_state  ─ Per-step content-aware premapping. For each
            │                mapping step, checks whether the step's own table
            │                is empty → Status='' (will sync) or has data →
            │                Status='Complete' (skip). CFG_UpgradeDataSync is
            │                not applicable to vp_xero_integration.
            ▼
  trigger_map_firm      ─ root entity (Firms first per docs).
            │              Each child DAG checks its step's Status; if
            │              'Complete' it short-circuits its sync. On success the
            │              child marks its step Status='Complete'.
            │
  trigger_map_account_code
            │
  trigger_map_tax_code
            │
  trigger_validate_mappings  ← Phase 5 (hard_fail keeps init=false; on
            │                    validation failure each step Status is set to
            │                    'Error')
            │
  gather_{firm,account_code,tax_code,validate_mappings}_error → combine_child_dag_errors →
     has_sync_errors ─yes→ fail_mapping_sync
                     └─no→ update_last_run_time →
                           mark_all_steps_ready (Workato 'Ready' handshake) →
                           mark_init_complete
                                  │
                           post_dag_run_details
"""
from vp_xero_integration_v2.common.tables import (
    ACCOUNT_TYPE_SEED_ROWS,
    MAP_ACCOUNT_TYPE_COLUMNS,
    MAP_ACCOUNT_TYPE_TABLE_NAME,
    MAP_ACCOUNT_TYPE_UNIQUE_COLUMNS,
    MAP_BANK_CODE_COLUMNS,
    MAP_BANK_CODE_TABLE_NAME,
    MAP_BANK_CODE_UNIQUE_COLUMNS,
    MAP_CHART_OF_ACCOUNTS_COLUMNS,
    MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
    MAP_CHART_OF_ACCOUNTS_UNIQUE_COLUMNS,
    MAP_CURRENCY_CODE_COLUMNS,
    MAP_CURRENCY_CODE_TABLE_NAME,
    MAP_CURRENCY_CODE_UNIQUE_COLUMNS,
    MAP_EMPLOYEE_COLUMNS,
    MAP_EMPLOYEE_TABLE_NAME,
    MAP_EMPLOYEE_UNIQUE_COLUMNS,
    MAP_FIRM_COLUMNS,
    MAP_FIRM_TABLE_NAME,
    MAP_FIRM_UNIQUE_COLUMNS,
    MAP_TAX_CODE_COLUMNS,
    MAP_TAX_CODE_TABLE_NAME,
    MAP_TAX_CODE_UNIQUE_COLUMNS,
    MAPPING_TABLE_STATE_COLUMNS,
    MAPPING_TABLE_STATE_TABLE_NAME,
    OUTSTANDING_EMPLOYEE_EXPENSES_COLUMNS,
    OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
    OUTSTANDING_PURCHASE_INVOICES_COLUMNS,
    OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
)
from vp_xero_integration_v2.mapping_sync.utils.python_callable_method import (
    apply_premapping_state,
    build_child_dag_conf as _build_child_dag_conf,
    is_mapping_init_complete,
    mark_all_steps_ready,
    mark_mapping_init_complete,
    seed_mapping_state_rows,
)
from vp_xero_integration_v2.mapping_sync.config import IntegrationConfig
from vp_xero_integration_v2.common.python_callable_method import (
    build_customer_variable_key,
    get_connections,
    utc_now_iso,
)
import logging
from datetime import timedelta
from airflow.models import Variable
import rail

_log = logging.getLogger(__name__)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,too-many-locals
def create_dag(config):
    """Create dispatcher DAG for one instance."""

    # Wrap the imported util to inject connections/customerId from the instance
    # file (V2 architecture: static config, not dag_run.conf at runtime).
    def build_child_dag_conf():
        conf = _build_child_dag_conf()
        # Override with instance file values when dag_run.conf is absent or empty
        # (setdefault is a no-op here — _build_child_dag_conf always sets both keys)
        conf['connections'] = conf.get('connections') or get_connections(config)
        conf['customerId'] = conf.get('customerId') or config.customer_id
        return conf

    with rail.create_airflow_dag(
        dag_id=IntegrationConfig.dag_id('dispatcher', config.instance),
        description='Per-customer mapping population dispatcher (map_firm → map_account_code → map_tax_code → validate_mappings)',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
        tags=['vantagepoint_xero', 'mapping_sync', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        # ----- Stage 0: per-customer init gate -----
        # Mapping population is a one-shot setup step. Once the per-customer
        # Variable `vp_xero_mapping_init_{customerId}_{instance}` is 'true', the
        # dispatcher skips every child DAG trigger. The Variable is flipped to
        # 'true' at the end of a successful run (see mark_mapping_init_complete
        # below). To force a re-run, manually delete the Variable (or set it to
        # 'false') in the Airflow UI.
        #
        # The IfOperator calls is_mapping_init_complete() directly rather than
        # routing the bool through XCom — some XCom backends serialize Python
        # `False` as the string "False", which is truthy in Python and would
        # incorrectly send the run to the skip branch.
        is_mapping_init_already_done = rail.IfOperator(
            task_id='is_mapping_init_already_done',
            test=lambda: is_mapping_init_complete(config.instance),
            yes_task='skip_mapping_init',
            no_task='init_mapping_collections',
        )

        skip_mapping_init = rail.PythonOperator(
            task_id='skip_mapping_init',
            python_callable=lambda: _log.info(
                'Mapping initialization already complete for this customer — '
                'skipping all child DAG triggers. Reset the '
                'vp_xero_mapping_init_<customerId>_<instance> Variable to '
                "'false' (or delete it) to force a re-run."
            ),
        )

        # ----- Stage 0.5: atomic multi-table init -----
        # One operator creates the mapping + state tables in a single S3
        # round-trip. The per-table preserve semantics handle idempotency:
        # re-runs see the tables already present with matching schemas and leave
        # them (and their seeded rows) alone.
        init_mapping_collections = rail.S3CreateMultiTableCollectionOperator(
            task_id='init_mapping_collections',
            integration=IntegrationConfig.S3_INTEGRATION_NAME,
            customer=IntegrationConfig.S3_CUSTOMER_TEMPLATE,
            integration_type='mapping_sync',
            tables=[
                # ----- Mapping tables -----
                {
                    'name': MAP_FIRM_TABLE_NAME,
                    'columns': MAP_FIRM_COLUMNS,
                    'unique_columns': MAP_FIRM_UNIQUE_COLUMNS,
                    'source': [],
                },
                {
                    'name': MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
                    'columns': MAP_CHART_OF_ACCOUNTS_COLUMNS,
                    'unique_columns': MAP_CHART_OF_ACCOUNTS_UNIQUE_COLUMNS,
                    'source': [],
                },
                {
                    'name': MAP_TAX_CODE_TABLE_NAME,
                    'columns': MAP_TAX_CODE_COLUMNS,
                    'unique_columns': MAP_TAX_CODE_UNIQUE_COLUMNS,
                    'source': [],
                },
                # ----- Seeded reference data -----
                # map_account_type is data-driven (Q7 = A): seeded here from the
                # Workato lookup-table data (ACCOUNT_TYPE_SEED_ROWS). The UNIQUE
                # index on XeroType + the per-table preserve semantics make the
                # seed idempotent across re-runs. `_account_sync` reads this as
                # the Xero-account-type → VP-type lookup at sync time.
                {
                    'name': MAP_ACCOUNT_TYPE_TABLE_NAME,
                    'columns': MAP_ACCOUNT_TYPE_COLUMNS,
                    'unique_columns': MAP_ACCOUNT_TYPE_UNIQUE_COLUMNS,
                    'source': [
                        {
                            'Description': description,
                            'XeroType': xero_type,
                            'VantagepointCode': vantagepoint_code,
                        }
                        for description, xero_type, vantagepoint_code
                        in ACCOUNT_TYPE_SEED_ROWS
                    ],
                },
                # ----- Sibling-integration collections (created up front; NOT
                # mapping_sync firm/account/tax steps — consumed by employee
                # sync, bank-code resolution, currency sync, and the GL
                # outstanding staging). Empty source; the map_* ones carry a
                # UNIQUE index for idempotent upserts, the outstanding_* ones do
                # not (transactional working state). -----
                {
                    'name': MAP_EMPLOYEE_TABLE_NAME,
                    'columns': MAP_EMPLOYEE_COLUMNS,
                    'unique_columns': MAP_EMPLOYEE_UNIQUE_COLUMNS,
                    'source': [],
                },
                {
                    'name': MAP_BANK_CODE_TABLE_NAME,
                    'columns': MAP_BANK_CODE_COLUMNS,
                    'unique_columns': MAP_BANK_CODE_UNIQUE_COLUMNS,
                    'source': [],
                },
                {
                    'name': MAP_CURRENCY_CODE_TABLE_NAME,
                    'columns': MAP_CURRENCY_CODE_COLUMNS,
                    'unique_columns': MAP_CURRENCY_CODE_UNIQUE_COLUMNS,
                    'source': [],
                },
                {
                    'name': OUTSTANDING_EMPLOYEE_EXPENSES_TABLE_NAME,
                    'columns': OUTSTANDING_EMPLOYEE_EXPENSES_COLUMNS,
                    'source': [],
                },
                {
                    'name': OUTSTANDING_PURCHASE_INVOICES_TABLE_NAME,
                    'columns': OUTSTANDING_PURCHASE_INVOICES_COLUMNS,
                    'source': [],
                },
                # ----- State table -----
                {
                    'name': MAPPING_TABLE_STATE_TABLE_NAME,
                    'columns': MAPPING_TABLE_STATE_COLUMNS,
                    # Seed the 3 mapping steps — firm, account, tax_code
                    # (Workato parity with `populate_mapping_state`). Per-table
                    # preserve semantics in the multi-table operator mean these
                    # rows land only on first create — on re-runs the existing
                    # rows are kept so Status values written by premapping /
                    # child marks / validate / final-ready persist.
                    'source': seed_mapping_state_rows(config.instance),
                },
            ],
        )

        # ----- Stage 0.75: premapping (per-step content-aware) -----
        # For each mapping step, checks whether the step's own table is empty
        # (Status='') or already has data (Status='Complete'). Each child DAG's
        # primary skip-gate reads Status via check_step_status. CFG_UpgradeDataSync
        # is not applicable to vp_xero_integration.
        premapping_state = rail.PythonOperator(
            task_id='apply_premapping_state',
            python_callable=apply_premapping_state,
        )

        # ----- Stage 1: Firms (root entity per Phase-3 docs) -----
        trigger_map_firm = rail.TriggerDagRunOperator(
            task_id='trigger_map_firm',
            retries=0,
            trigger_dag_id=IntegrationConfig.dag_id(
                'map_firm', config.instance),
            conf=build_child_dag_conf,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ----- Stage 2: Accounts -----
        trigger_map_account_code = rail.TriggerDagRunOperator(
            task_id='trigger_map_account_code',
            retries=0,
            trigger_dag_id=IntegrationConfig.dag_id(
                'map_account_code', config.instance),
            conf=build_child_dag_conf,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ----- Stage 3: Tax Codes -----
        trigger_map_tax_code = rail.TriggerDagRunOperator(
            task_id='trigger_map_tax_code',
            retries=0,
            trigger_dag_id=IntegrationConfig.dag_id(
                'map_tax_code', config.instance),
            conf=build_child_dag_conf,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ----- Stage 4: validation (Phase 5) -----
        # Runs AFTER all mapping populates so the validators see the final state
        # of map_*. Hard-fail validations cause this child DAG to fail; the
        # dispatcher's gather chain picks the failure up via
        # gather_validate_mappings_error → has_sync_errors → fail_mapping_sync,
        # which means mark_mapping_init_complete is skipped and the init Variable
        # stays 'false' so the next run retries from scratch.
        #
        # `trigger_rule='none_skipped'` (NOT 'all_done'): we want validate to
        # run after any pipeline pass-through (success or internal child-DAG
        # failure absorbed by its catch task), but NOT when the pipeline was
        # bypassed entirely via the `skip_mapping_init` branch — under 'all_done'
        # SKIPPED upstream still counts as "done", which fired validate on the
        # skip path. Child DAGs always return SUCCESS at the trigger level
        # because their internal catch_*_dag_error tasks absorb failures, so
        # 'none_skipped' preserves the original "validate runs even on partial
        # failure" intent while correctly skipping on the bypass path.
        trigger_validate_mappings = rail.TriggerDagRunOperator(
            task_id='trigger_validate_mappings',
            retries=0,
            trigger_dag_id=IntegrationConfig.dag_id(
                'validate_mappings', config.instance),
            conf=build_child_dag_conf,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_rule='none_skipped',
        )

        # ----- Error gather: read the catch_* dict from each child DAG -----
        # Each child DAG exposes a different catch task ID, so collect them
        # individually then concatenate via a Python step (no single gather call
        # can read varied dagrun_task_ids in one shot).
        gather_map_firm_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_map_firm_error',
            dag_runs="{{ result('trigger_map_firm') }}",
            dagrun_task_id='catch_map_firm_dag_error',
            flatten=True,
        )
        gather_map_account_code_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_map_account_code_error',
            dag_runs="{{ result('trigger_map_account_code') }}",
            dagrun_task_id='catch_map_account_code_dag_error',
            flatten=True,
        )
        gather_map_tax_code_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_map_tax_code_error',
            dag_runs="{{ result('trigger_map_tax_code') }}",
            dagrun_task_id='catch_map_tax_code_dag_error',
            flatten=True,
        )
        gather_validate_mappings_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_validate_mappings_error',
            dag_runs="{{ result('trigger_validate_mappings') }}",
            dagrun_task_id='catch_validate_mappings_dag_error',
            flatten=True,
        )

        def combine_child_errors():
            """Flatten the per-child error lists into a single list of non-None dicts."""
            sources = [
                rail.result('gather_map_firm_error'),
                rail.result('gather_map_account_code_error'),
                rail.result('gather_map_tax_code_error'),
                rail.result('gather_validate_mappings_error'),
            ]
            errors = []
            for src in sources:
                if not src:
                    continue
                if isinstance(src, list):
                    errors.extend([e for e in src if e])
                elif isinstance(src, dict):
                    errors.append(src)
            return errors

        # `trigger_rule='none_skipped'` (NOT 'all_done'): same skip-path fix as
        # trigger_validate_mappings above. With 'all_done', all gather_*_error
        # tasks being SKIPPED (the bypass case) still counted as "done" and
        # combine_errors fired with an empty error list — that propagated through
        # has_sync_errors → No branch → update_last_run_time → mark_init_complete,
        # falsely re-confirming init-complete on every skip-path run.
        # `none_skipped` correctly cascades the SKIPPED state through the whole
        # has_sync_errors / update_last_run / mark_* subgraph, leaving only
        # post_dag_run_details to fire (it has its own 'all_done' rule for
        # middleware notification). Normal failure-absorbed-by-catch paths still
        # have SUCCESS upstream — so the original "always run combine on
        # completion" intent is preserved without the skip-path leak.
        combine_errors = rail.PythonOperator(
            task_id='combine_child_dag_errors',
            python_callable=combine_child_errors,
            trigger_rule='none_skipped',
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test="{{ result('combine_child_dag_errors') | length > 0 }}",
            yes_task='fail_mapping_sync',
            no_task='update_last_run_time',
        )

        fail_mapping_sync = rail.FailOperator(
            task_id='fail_mapping_sync',
            message=(
                "{{ result('combine_child_dag_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            ),
        )

        def update_last_run_time():
            """Record the timestamp of this successful population run per-customer."""
            customer_id = (
                rail.get_current_context()['dag_run'].conf.get('customerId')
            )
            variable_key = build_customer_variable_key(
                customer_id, 'mapping_sync_last_run'
            )
            current_time = utc_now_iso()
            Variable.set(variable_key, current_time)
            _log.info(
                "Updated last run time Variable '%s' to: %s",
                variable_key, current_time,
            )
            return current_time

        update_last_run = rail.PythonOperator(
            task_id='update_last_run_time',
            python_callable=update_last_run_time,
        )

        # Final Ready handshake — Workato parity with the validate-mapping-tables
        # 'Ready' step. Bulk-sets mapping_table_state.Status='Ready' on all 3
        # step rows (firm, account, tax_code) once validation passes. Runs on the
        # no-errors success path only.
        mark_steps_ready = rail.PythonOperator(
            task_id='mark_all_steps_ready',
            python_callable=mark_all_steps_ready,
        )

        # Flip the per-customer init Variable to 'true' on the success path only
        # (downstream of update_last_run_time, which is downstream of
        # has_sync_errors=No). Any child-DAG error short-circuits to
        # fail_mapping_sync and this task is skipped, leaving the Variable at
        # 'false' so the next dispatcher run retries from scratch.
        mark_init_complete = rail.PythonOperator(
            task_id='mark_mapping_init_complete',
            python_callable=mark_mapping_init_complete,
            op_args=[config.instance],
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url="{{ var.value.get('middleware_api_base_url', '') }}",
            trigger_rule='all_done',
        )

        # ----- Dependencies -----

        # Stage 0 → Stage 0.5 (init gate). The IfOperator has no upstream
        # producer task — it evaluates its callable test at branch time.
        (
            is_mapping_init_already_done >>
            rail.Label('Already initialized') >>
            skip_mapping_init
        )
        (
            is_mapping_init_already_done >>
            rail.Label('Not initialized') >>
            init_mapping_collections >>
            premapping_state >>
            trigger_map_firm
        )

        # Strict-sequence master-data chain (Phase-3 order),
        # then validation (Phase-5) gates the success path.
        (
            trigger_map_firm >>
            trigger_map_account_code >>
            trigger_map_tax_code >>
            trigger_validate_mappings
        )

        # Error gather: each gather reads its own trigger task's child runs.
        trigger_map_firm >> gather_map_firm_error
        trigger_map_account_code >> gather_map_account_code_error
        trigger_map_tax_code >> gather_map_tax_code_error
        trigger_validate_mappings >> gather_validate_mappings_error

        [
            gather_map_firm_error,
            gather_map_account_code_error,
            gather_map_tax_code_error,
            gather_validate_mappings_error,
        ] >> combine_errors >> has_sync_errors

        has_sync_errors >> rail.Label('Yes') >> fail_mapping_sync
        has_sync_errors >> rail.Label('No') >> update_last_run

        # Success path: update_last_run → mark_steps_ready (Workato parity
        # 'Ready' handshake) → mark_init_complete (flip the per-customer init
        # Variable) → post_dag_run_details. mark_init_complete is wired only off
        # the no-error branch, so any child-DAG failure (which routes to
        # fail_mapping_sync) leaves the per-customer init Variable at 'false' and
        # the next dispatcher run retries from scratch. Fail/skip paths converge
        # at post_dag_run_details via its 'all_done' trigger rule.
        update_last_run >> mark_steps_ready >> mark_init_complete >> post_dag_run_details
        fail_mapping_sync >> post_dag_run_details
        skip_mapping_init >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)

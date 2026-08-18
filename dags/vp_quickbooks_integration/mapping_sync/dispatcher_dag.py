"""
Dispatcher DAG for VP QBO Mapping Sync.

Per-customer orchestrator. Creates the S3 mapping tables, seeds the
mapping_table_state rows, applies premapping (CFG_UpgradeDataSync
gate), triggers the five mapping_sync child DAGs in priority order,
gathers any child errors, and either fails the run or posts a
successful run-details payload + final 'Ready' state handshake.

Sequence (per integration_vantagepoint_quickbooks/docs strict Phase-3 order):
  is_mapping_init_already_done  ─ per-customer one-shot init gate.
            │ (Not initialized)
  init_mapping_collections  ─ S3CreateMultiTableCollectionOperator.
            │                  Creates the collection tables and seeds
            │                  mapping_table_state (4 step rows: firm,
            │                  employee, account_code, tax_code)
            │                  in one S3 round-trip. (account_type_map is
            │                  no longer a collection — it's the static
            │                  ACCOUNT_TYPE_MAP Python constant.)
            ▼
  apply_premapping_state  ─ Workato premapping parity. Reads
            │                CFG_UpgradeDataSync from dag_run.conf
            │                and sets mapping_table_state.Status to
            │                'Complete' (false → skip) or '' (true →
            │                force re-run) on all 4 step rows.
            ▼
  trigger_map_firm      ─ root entity (Firms first per docs).
            │              Each child DAG checks its step's Status;
            │              if 'Complete' it short-circuits its sync.
            │              On success the child marks its step
            │              Status='Complete'.
            │
  trigger_map_employee  ─ depends on firms (vendor association)
            │
  trigger_map_account_code
            │
  trigger_map_tax_code
            │
  trigger_validate_mappings  ← Phase 5 (hard_fail keeps init=false;
            │                    on validation failure each step
            │                    Status is set to 'Error')
            │
  wait_for_child_dag_runs → gather_child_dag_errors →
     has_sync_errors ─yes→ fail_mapping_sync
                     └─no→ update_last_run_time →
                           mark_all_steps_ready (Workato 'Ready'
                                                 handshake) →
                           mark_init_complete
                                  │
                           post_dag_run_details
"""
from vp_quickbooks_integration.common.tables import (
    BANK_CODE_MAP_COLUMNS,
    BANK_CODE_MAP_TABLE_NAME,
    MAP_ACCOUNT_CODE_COLUMNS,
    MAP_ACCOUNT_CODE_TABLE_NAME,
    MAP_ACCOUNT_CODE_UNIQUE_COLUMNS,
    MAP_EMPLOYEE_COLUMNS,
    MAP_EMPLOYEE_TABLE_NAME,
    MAP_EMPLOYEE_UNIQUE_INDEXES,
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
    OUTSTANDING_SALES_INVOICES_COLUMNS,
    OUTSTANDING_SALES_INVOICES_TABLE_NAME,
)
from vp_quickbooks_integration.common.python_callable_method import (
    build_customer_variable_key,
)
from vp_quickbooks_integration.mapping_sync.utils.python_callable_method import (
    apply_premapping_state,
    build_child_dag_conf,
    is_mapping_init_complete,
    mark_all_steps_ready,
    mark_mapping_init_complete,
    seed_mapping_state_rows,
)
from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig
import logging
from datetime import datetime, timezone, timedelta
from airflow.models import Variable
import rail

_log = logging.getLogger(__name__)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned,too-many-locals
def create_dag(config):
    """Create dispatcher DAG for one instance."""
    with rail.create_airflow_dag(
        dag_id=IntegrationConfig.dag_id('dispatcher', config.instance),
        description='Per-customer mapping population dispatcher (orchestrates 5 child DAGs)',
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        tags=['vantagepoint_quickbooks', 'mapping_sync', 'dispatcher'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        # ----- Stage 0: per-customer init gate -----
        # Mapping population is a one-shot setup step. Once the per-customer
        # Variable `vp_qbo_{customerId}_mapping_init` is 'true',
        # the dispatcher skips every child DAG trigger. The Variable is
        # flipped to 'true' at the end of a successful run (see
        # mark_mapping_init_complete below). To force a re-run, manually
        # delete the Variable (or set it to 'false') in the Airflow UI.
        #
        # The IfOperator calls is_mapping_init_complete() directly rather
        # than routing the bool through XCom — some XCom backends serialize
        # Python `False` as the string "False", which is truthy in Python
        # and would incorrectly send the run to the skip branch.
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
                'vp_qbo_<customerId>_mapping_init Variable to '
                "'false' (or delete it) to force a re-run."
            ),
        )

        # ----- Stage 0.5: atomic multi-table init -----
        # One operator creates the mapping/outstanding/state/config tables
        # in a single S3 round-trip. The per-table preserve semantics
        # handle idempotency: re-runs see the tables already present with
        # matching schemas and leave them alone. (account_type_map is no
        # longer created here — it's the static ACCOUNT_TYPE_MAP Python
        # constant in common/tables.py, like PAY_TERMS_MAP.)
        init_mapping_collections = rail.S3CreateMultiTableCollectionOperator(
            task_id='init_mapping_collections',
            integration=IntegrationConfig.S3_INTEGRATION_NAME,
            customer=IntegrationConfig.S3_CUSTOMER_TEMPLATE,
            integration_type=IntegrationConfig.S3_INTEGRATION_TYPE_TEMPLATE,
            tables=[
                # ----- Mapping tables -----
                {
                    'name': MAP_FIRM_TABLE_NAME,
                    'columns': MAP_FIRM_COLUMNS,
                    'unique_columns': MAP_FIRM_UNIQUE_COLUMNS,
                    'source': [],
                },
                {
                    'name': MAP_EMPLOYEE_TABLE_NAME,
                    'columns': MAP_EMPLOYEE_COLUMNS,
                    # Two independent UNIQUE indexes (QBOID, Employee) — see
                    # MAP_EMPLOYEE_UNIQUE_INDEXES. Both writers (employee_sync
                    # by QBOID, employee_sync_upsert by Employee) upsert atomically.
                    'unique_columns': MAP_EMPLOYEE_UNIQUE_INDEXES,
                    'source': [],
                },
                {
                    'name': MAP_ACCOUNT_CODE_TABLE_NAME,
                    'columns': MAP_ACCOUNT_CODE_COLUMNS,
                    'unique_columns': MAP_ACCOUNT_CODE_UNIQUE_COLUMNS,
                    'source': [],
                },
                {
                    'name': MAP_TAX_CODE_TABLE_NAME,
                    'columns': MAP_TAX_CODE_COLUMNS,
                    'unique_columns': MAP_TAX_CODE_UNIQUE_COLUMNS,
                    'source': [],
                },
                # ----- Outstanding / state tables -----
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
                {
                    'name': OUTSTANDING_SALES_INVOICES_TABLE_NAME,
                    'columns': OUTSTANDING_SALES_INVOICES_COLUMNS,
                    'source': [],
                },
                {
                    'name': MAPPING_TABLE_STATE_TABLE_NAME,
                    'columns': MAPPING_TABLE_STATE_COLUMNS,
                    # Seed the 4 mapping steps — firm, employee,
                    # account_code, tax_code (Workato parity with
                    # `populate_mapping_state.recipe.json`). Per-table
                    # preserve semantics in the multi-table operator
                    # mean these rows land only on first create — on
                    # re-runs the existing rows are kept so Status
                    # values written by premapping / child marks /
                    # validate / final-ready persist.
                    'source': seed_mapping_state_rows(config.instance),
                },
                # ----- Configuration tables -----
                # pay_terms and invoice_section_code are NOT created here:
                # they are static, read-only config shipped as Python constants
                # (PAY_TERMS_MAP / INVOICE_SECTION_CODE_MAP in utils/tables.py),
                # not S3 collections. See doc/STATIC_CONFIG_LOOKUPS.md.
                {
                    'name': BANK_CODE_MAP_TABLE_NAME,
                    'columns': BANK_CODE_MAP_COLUMNS,
                    'source': [],
                },
            ],
        )

        # ----- Stage 0.75: premapping (Workato parity) -----
        # Reads `CFG_UpgradeDataSync` from dag_run.conf.config. When
        # 'false' (Workato default), sets Status='Complete' on all 4
        # step rows — each child DAG's skip-gate then short-circuits.
        # When 'true' (data migration mode), clears Status so children
        # run as normal. Mirrors
        # `014_503_psa_premapping.recipe.json` lines 436-454.
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

        # ----- Stage 2: Employees (after firms; vendor association required) -----
        trigger_map_employee = rail.TriggerDagRunOperator(
            task_id='trigger_map_employee',
            retries=0,
            trigger_dag_id=IntegrationConfig.dag_id(
                'map_employee', config.instance),
            conf=build_child_dag_conf,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ----- Stage 3: Accounts -----
        trigger_map_account_code = rail.TriggerDagRunOperator(
            task_id='trigger_map_account_code',
            retries=0,
            trigger_dag_id=IntegrationConfig.dag_id(
                'map_account_code', config.instance),
            conf=build_child_dag_conf,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ----- Stage 4: Tax Codes -----
        trigger_map_tax_code = rail.TriggerDagRunOperator(
            task_id='trigger_map_tax_code',
            retries=0,
            trigger_dag_id=IntegrationConfig.dag_id(
                'map_tax_code', config.instance),
            conf=build_child_dag_conf,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ----- Stage 5: validation (Phase 5) -----
        # Runs AFTER all mapping populates so the validators see the
        # final state of map_*. Hard-fail validations cause this child
        # DAG to fail; the dispatcher's gather chain picks the failure
        # up via gather_validate_mappings_error → has_sync_errors →
        # fail_mapping_sync, which means mark_mapping_init_complete is
        # skipped and the init Variable stays 'false' so the next run
        # retries from scratch.
        #
        # `trigger_rule='none_skipped'` (NOT 'all_done'): we want
        # validate to run after any pipeline pass-through (success or
        # internal child-DAG failure absorbed by its catch task), but
        # NOT when the pipeline was bypassed entirely via the
        # `skip_mapping_init` branch — under 'all_done' SKIPPED upstream
        # still counts as "done", which fired validate on the skip path
        # (regression: validate would open the S3 collection and
        # potentially hard_fail on transient empty-table reads for an
        # already-initialised customer). Child DAGs always return
        # SUCCESS at the trigger level because their internal
        # catch_*_dag_error tasks absorb failures, so 'none_skipped'
        # preserves the original "validate runs even on partial
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
        gather_map_employee_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_map_employee_error',
            dag_runs="{{ result('trigger_map_employee') }}",
            dagrun_task_id='catch_map_employee_dag_error',
            flatten=True,
        )
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
                rail.result('gather_map_employee_error'),
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

        # `trigger_rule='none_skipped'` (NOT 'all_done'): same skip-path
        # fix as trigger_validate_mappings above. With 'all_done', all
        # gather_*_error tasks being SKIPPED (the bypass case) still
        # counted as "done" and combine_errors fired with an empty
        # error list — that propagated through has_sync_errors → No
        # branch → update_last_run_time → mark_init_complete, falsely
        # re-confirming init-complete on every skip-path run.
        # `none_skipped` correctly cascades the SKIPPED state through
        # the whole has_sync_errors / update_last_run / mark_*
        # subgraph, leaving only post_dag_run_details to fire (it has
        # its own 'all_done' rule for middleware notification).
        # Normal failure-absorbed-by-catch paths still have SUCCESS
        # upstream — so the original "always run combine on completion"
        # intent is preserved without the skip-path leak.
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
            current_time = (
                datetime.now(timezone.utc)
                .strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            )
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

        # Final Ready handshake — Workato parity with
        # `014_503_psa_validate_mapping_tables.recipe.json` line 2192.
        # Bulk-sets mapping_table_state.Status='Ready' on all 4 step
        # rows (firm, employee, account_code, tax_code) once validation
        # passes. Runs on the no-errors success path only.
        mark_steps_ready = rail.PythonOperator(
            task_id='mark_all_steps_ready',
            python_callable=mark_all_steps_ready,
        )

        # Flip the per-customer init Variable to 'true' on the success path
        # only (downstream of update_last_run_time, which is downstream of
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
            middleware_api_base_url=Variable.get(
                'middleware_api_base_url', default_var=''
            ),
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
            trigger_map_employee >>
            trigger_map_account_code >>
            trigger_map_tax_code >>
            trigger_validate_mappings
        )

        # Error gather: each gather reads its own trigger task's child runs.
        trigger_map_employee >> gather_map_employee_error
        trigger_map_firm >> gather_map_firm_error
        trigger_map_account_code >> gather_map_account_code_error
        trigger_map_tax_code >> gather_map_tax_code_error
        trigger_validate_mappings >> gather_validate_mappings_error

        [
            gather_map_employee_error,
            gather_map_firm_error,
            gather_map_account_code_error,
            gather_map_tax_code_error,
            gather_validate_mappings_error,
        ] >> combine_errors >> has_sync_errors

        has_sync_errors >> rail.Label('Yes') >> fail_mapping_sync
        has_sync_errors >> rail.Label('No') >> update_last_run

        # Success path: update_last_run → mark_steps_ready (Workato
        # parity 'Ready' handshake) → mark_init_complete (flip the
        # per-customer init Variable) → post_dag_run_details.
        # mark_init_complete is wired only off the no-error branch, so any
        # child-DAG failure (which routes to fail_mapping_sync) leaves the
        # per-customer init Variable at 'false' and the next dispatcher run
        # retries from scratch.
        # Fail/skip paths converge at post_dag_run_details via its 'all_done'
        # trigger rule.
        update_last_run >> mark_steps_ready >> mark_init_complete >> post_dag_run_details
        fail_mapping_sync >> post_dag_run_details
        skip_mapping_init >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)

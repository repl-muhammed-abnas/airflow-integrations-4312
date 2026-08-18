"""
Shared infrastructure for VP QBO mapping_sync.

S3 collection access primitives, per-customer mapping-init Variable
gate, mapping_table_state lifecycle helpers, child DAG conf builder,
skip-gate helpers, error capture, and QBO/VP response normalisers.

Every per-table sync module (`_firm_sync.py`, `_employee_sync.py`,
`_account_sync.py`, `_tax_code_sync.py`,
`_validate.py`) imports from here. `python_callable_method.py` is a
thin shim that re-exports the public surface for backwards-compat
with existing DAG imports.

Public surface (re-exported via `python_callable_method.py`):
    open_mapping_collection
    is_mapping_init_complete
    mark_mapping_init_complete
    seed_mapping_state_rows
    apply_premapping_state
    mark_step_status
    check_step_status
    mark_all_steps_ready
    build_child_dag_conf
    count_collection_rows
    is_table_populated
    capture_dag_error
"""
import logging
from contextlib import contextmanager

import rail
from airflow.models import Variable
from rail import S3QueryCollectionOperator, S3UpdateCollectionOperator

from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig
from vp_quickbooks_integration.common.python_callable_method import (
    build_customer_variable_key,
)
from vp_quickbooks_integration.common.tables import (
    MAP_ACCOUNT_CODE_TABLE_NAME,
    MAP_EMPLOYEE_TABLE_NAME,
    MAP_FIRM_TABLE_NAME,
    MAP_TAX_CODE_TABLE_NAME,
)


# Module-level logger for helpers that are called as PythonOperator
# callbacks. Sync helpers grab the task-instance log via
# `rail.get_current_context()['task_instance'].log` for per-task-attempt
# grouping; the module logger covers helpers where the context plumbing
# isn't worth it.
_log = logging.getLogger(__name__)


def _filter_none(body):
    """Drop keys whose value is None. Empty strings are KEPT (recipe parity)."""
    return {k: v for k, v in body.items() if v is not None}


# ---------------------------------------------------------------------------
# Per-customer "mapping initialized" gate
#
# Mapping population is an integration-setup step, not an ongoing sync. Once
# the per-customer Variable `vp_qbo_{customerId}_mapping_init` is
# 'true', the dispatcher skips all child DAG triggers. The Variable is set
# by mark_mapping_init_complete() at the end of a successful dispatcher run
# (no child-DAG errors); failed runs leave it 'false' so the next run retries.
# ---------------------------------------------------------------------------

# Suffix for the per-customer mapping-init gate; the full key format
# (`vp_qbo_<customer>_<suffix>`) lives in common.build_customer_variable_key.
_MAPPING_INIT_SUFFIX = 'mapping_init'


# ---------------------------------------------------------------------------
# Shared S3 collection access primitives
# ---------------------------------------------------------------------------
# Two patterns recurred 9+ times across this module:
#
#   1. open the artifact via get_or_create_s3_collection_artifact, do
#      sqlite work, let the context manager upload on exit (the
#      WRITEABLE pattern), or
#
#   2. download + decompress directly, do read-only sqlite work, drop
#      the temp dir (the READ-ONLY pattern, used by the 4 validators —
#      bypasses the upload to avoid wasted writes + parallel races).
#
# Both patterns repeat the same 5-line "resolve integration/customer/
# integration_type from context" lookup. `_resolve_s3_locator` and
# `open_mapping_collection` consolidate both shapes behind a single
# context manager — callers go from ~12 lines of boilerplate per method
# to a 2-line `with open_mapping_collection(...) as conn:` block.
# ---------------------------------------------------------------------------


def _resolve_s3_locator(context=None):
    """Resolve the (integration, customer, integration_type) triple from
    the active dag_run context. Returns a dict ready to pass into the
    S3 collection operators / artifact helpers.
    """
    context = context or rail.get_current_context()
    return {
        'integration': IntegrationConfig.S3_INTEGRATION_NAME,
        'customer': IntegrationConfig.get_s3_customer(context),
        'integration_type': IntegrationConfig.get_s3_integration_type(context),
        'context': context,
    }


@contextmanager
def open_mapping_collection(*, read_only=False):
    """Yield a sqlite3 connection to the current dag_run's mapping
    collection.

    ``read_only=False`` (default): goes through
    ``rail.lib.s3_collection.get_or_create_s3_collection_artifact`` —
    downloads the artifact, yields, and uploads on exit (skipped
    automatically when the DB is unchanged thanks to the RAIL hash
    short-circuit). Use this for any caller that writes to the
    collection or for atomic check-then-write flows that need both
    reads and writes to run inside a single sqlite session.

    ``read_only=True``: downloads + decompresses directly via
    ``download_from_s3`` / ``decompress_file`` and never uploads.
    Used by the 4 validators (`validate_map_firm` etc.); avoids both
    the wasted write and the ETag race when multiple parallel readers
    touch the same key.

    Resolves integration / customer / integration_type from
    ``rail.get_current_context()`` so callers don't repeat the lookup.
    """
    # pylint: disable=import-outside-toplevel
    import os
    import sqlite3
    import shutil
    import tempfile
    import rail.lib.s3_collection

    locator = _resolve_s3_locator()
    context = locator['context']
    s3_integration = locator['integration']
    s3_customer = locator['customer']
    s3_integration_type = locator['integration_type']

    if read_only:
        s3_key = rail.lib.s3_collection.build_s3_key(
            s3_integration, s3_customer, s3_integration_type,
        )
        temp_dir = tempfile.mkdtemp()
        try:
            gz_path = os.path.join(temp_dir, 'collections.db.gz')
            db_path = os.path.join(temp_dir, 'collections.db')
            rail.lib.s3_collection.download_from_s3(s3_key, gz_path)
            rail.lib.s3_collection.decompress_file(gz_path, db_path)
            with sqlite3.connect(db_path) as conn:
                yield conn
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    else:
        s3_artifact_name = (
            rail.lib.s3_collection.get_s3_collection_artifact_name(
                context, s3_integration, s3_customer, s3_integration_type,
            )
        )
        with rail.lib.s3_collection.get_or_create_s3_collection_artifact(
            s3_artifact_name, s3_integration, s3_customer, context,
            integration_type=s3_integration_type,
        ) as artifact:
            with sqlite3.connect(artifact.local_filename) as conn:
                yield conn


def _mapping_init_variable_key(instance=None):  # pylint: disable=unused-argument
    """Build the per-customer mapping-init Variable key.

    `instance` is retained for call-site compatibility but no longer part of
    the key (the gate is per-customer, not per-instance). customer_id is
    sanitized by build_customer_variable_key.
    """
    customer_id = (
        rail.get_current_context()['dag_run'].conf.get('customerId') or ''
    )
    return build_customer_variable_key(customer_id, _MAPPING_INIT_SUFFIX)


def is_mapping_init_complete(instance):
    """
    Return True iff the per-customer mapping-init Variable is set to 'true'.

    Default value is 'false' (case-insensitive). Used by the dispatcher's
    `check_mapping_init_status` task at the top of the DAG to short-circuit
    repeat runs.
    """
    try:
        variable_key = _mapping_init_variable_key(instance)
        raw = Variable.get(variable_key, default_var='false')
        is_done = str(raw).strip().lower() == 'true'
        _log.info(
            "Mapping init Variable '%s' = %r (%s)",
            variable_key,
            raw,
            'already initialized — skipping' if is_done else 'not yet initialized — proceeding',
        )
        return is_done
    except Exception as exc:
        _log.warning(
            "Error checking mapping init status for instance '%s': %s. "
            "Defaulting to False (proceed with initialization).",
            instance, exc
        )
        return False


def mark_mapping_init_complete(instance):
    """
    Set the per-customer mapping-init Variable to 'true' after a successful run.

    Called by the dispatcher's `mark_mapping_init_complete` task, which is wired
    on the no-errors branch only (downstream of `update_last_run_time`). A run
    with any child-DAG error never reaches this task, so the Variable stays
    'false' and the next run retries the whole population.
    """
    variable_key = _mapping_init_variable_key(instance)
    Variable.set(variable_key, 'true')
    _log.info("Marked mapping init complete: Variable '%s' = 'true'", variable_key)
    return variable_key


# ---------------------------------------------------------------------------
# Child DAG conf builder (used by dispatcher_dag's TriggerDagRunOperators)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# mapping_table_state helpers (Workato `populate_mapping_state` parity)
# ---------------------------------------------------------------------------

def seed_mapping_state_rows(instance):
    """Build the 4 seed rows for `mapping_table_state`.

    Consumed by `dispatcher_dag.init_mapping_collections` as the
    `source` argument for the `mapping_table_state` table spec. Workato
    parity with `014_503_psa_populate_mapping_state.recipe.json` lines
    139-164. Status (col4) is intentionally blank on seed — it gets
    set by `apply_premapping_state` before child DAGs trigger.
    """
    # pylint: disable=import-outside-toplevel
    from vp_quickbooks_integration.common.tables import (
        MAPPING_STEPS_ORDERED,
    )
    return [
        {
            'Step': step,
            'DagId': IntegrationConfig.dag_id(_step_dag_role(step), instance),
            'TableName': table_name,
            'Status': '',
            'Messages': '',
            'Sequence': sequence,
        }
        for step, table_name, sequence in MAPPING_STEPS_ORDERED
    ]


def _step_dag_role(step):
    """Map a Workato Step label to the matching Airflow DAG role suffix
    (the second argument to `IntegrationConfig.dag_id`)."""
    # pylint: disable=import-outside-toplevel
    from vp_quickbooks_integration.common.tables import (
        MAPPING_STEP_FIRM, MAPPING_STEP_EMPLOYEE,
        MAPPING_STEP_ACCOUNT, MAPPING_STEP_TAX_CODE,
    )
    return {
        MAPPING_STEP_FIRM: 'map_firm',
        MAPPING_STEP_EMPLOYEE: 'map_employee',
        MAPPING_STEP_ACCOUNT: 'map_account_code',
        MAPPING_STEP_TAX_CODE: 'map_tax_code',
    }[step]


def _update_mapping_state_status(step, status, message=''):
    """UPDATE mapping_table_state.Status + Messages for one Step.

    Thin wrapper around ``S3UpdateCollectionOperator`` — keeps a stable
    callable surface so callers (`mark_step_status`,
    `summarize_mapping_validations`) don't have to construct operators
    inline. The operator handles the artifact open/upload cycle,
    including the no-op upload skip when nothing changed.
    """
    # pylint: disable=import-outside-toplevel
    from vp_quickbooks_integration.common.tables import (
        MAPPING_TABLE_STATE_TABLE_NAME,
    )

    locator = _resolve_s3_locator()
    S3UpdateCollectionOperator(
        task_id=f'_update_mapping_state_{step.replace(" ", "_").lower()}',
        integration=locator['integration'],
        customer=locator['customer'],
        integration_type=locator['integration_type'],
        collection_name=MAPPING_TABLE_STATE_TABLE_NAME,
        query=(
            f'UPDATE {MAPPING_TABLE_STATE_TABLE_NAME} '
            f'SET Status = ?, Messages = ? WHERE Step = ?'
        ),
        query_params=[status, message, step],
    ).execute(locator['context'])
    return {'step': step, 'status': status, 'messages': message}


def _read_mapping_state_row(step):
    """Read the current (Status, Messages) tuple for one Step.

    Thin wrapper around ``S3QueryCollectionOperator(mode='single-row')``
    — same pattern as ``count_collection_rows``. Returns ``('', '')``
    when the row is missing or the table doesn't exist yet (the
    operator surfaces both as exceptions; we treat them as "unknown
    step state, fall through to populate path" for the caller's
    perspective).
    """
    # pylint: disable=import-outside-toplevel
    from vp_quickbooks_integration.common.tables import (
        MAPPING_TABLE_STATE_TABLE_NAME,
    )

    locator = _resolve_s3_locator()
    query_op = S3QueryCollectionOperator(
        task_id=f'_read_mapping_state_{step.replace(" ", "_").lower()}',
        query=(
            f'SELECT Status, Messages '
            f'FROM {MAPPING_TABLE_STATE_TABLE_NAME} '
            f'WHERE Step = ? LIMIT 1'
        ),
        query_params=[step],
        integration=locator['integration'],
        customer=locator['customer'],
        integration_type=locator['integration_type'],
        mode='single-row',
    )
    try:
        row = query_op.execute(locator['context'])
    except FileNotFoundError:
        return ('', '')
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if 'no such table' in str(exc).lower():
            return ('', '')
        raise
    if not row:
        return ('', '')
    if isinstance(row, dict):
        return (row.get('Status', '') or '', row.get('Messages', '') or '')
    try:
        return (row[0] or '', row[1] or '')
    except (TypeError, IndexError):
        return ('', '')


def apply_premapping_state():
    """Workato `014_503_psa_premapping.recipe.json` equivalent.

    Reads `CFG_UpgradeDataSync` from the dispatcher's dag_run.conf
    (forwarded by main_dag) and decides each of the 4 mapping steps'
    initial Status — the Workato-parity steps (firm / employee /
    account_code / tax_code):

      - CFG_UpgradeDataSync == 'true' (data migration mode)
        → Status = '' (clear; force re-run)
      - CFG_UpgradeDataSync == 'false' (default; strict Workato parity)
        → Status = 'Complete' (skip the sync; data already in place)

    Airflow-side content-aware override (NOT in Workato): when
    `CFG=false` but all 4 mapping tables are empty post-init (e.g.
    fresh customer, or operator wiped the S3 collection to force a
    re-run), there is no "external data" to trust — so the effective
    behavior switches to `CFG=true` semantics (Status='') and the
    syncs run. Strict Workato parity is preserved only when the
    tables actually have content: in that case `CFG=false` does the
    "trust existing data" skip.

    This makes "delete S3 collection + reset init Variable" the
    natural way to force a re-sync, without needing to flip the
    middleware-side CFG flag.

    Called as a PythonOperator task in the dispatcher between
    `init_mapping_collections` and `trigger_map_firm`. Per-step skip
    gates read Status via `check_step_status`.
    """
    # pylint: disable=import-outside-toplevel
    from vp_quickbooks_integration.common.tables import (
        MAPPING_STEPS_ORDERED, MAPPING_TABLE_STATE_TABLE_NAME,
    )

    context = rail.get_current_context()
    cfg = IntegrationConfig.get_cfg(context, 'CFG_UpgradeDataSync')
    # Coerce to canonical 'true'/'false'. Middleware ships booleans
    # (Python `True`/`False`); Workato evaluates the string form.
    is_upgrade = str(cfg).strip().lower() == 'true'

    overridden = False
    with open_mapping_collection() as conn:
        cur = conn.cursor()

        # Content-aware override: skip the "trust existing data" branch
        # when there is no data to trust. Inspecting all 5 mapping
        # tables (iterates MAPPING_STEPS_ORDERED) inside the same
        # sqlite session avoids extra S3 round-trips.
        if not is_upgrade:
            all_empty = True
            for _step, table_name, _sequence in MAPPING_STEPS_ORDERED:
                row = cur.execute(
                    f'SELECT 1 FROM {table_name} LIMIT 1'
                ).fetchone()
                if row is not None:
                    all_empty = False
                    break
            if all_empty:
                overridden = True
                is_upgrade = True

        target_status = '' if is_upgrade else 'Complete'
        if overridden:
            message = (
                'CFG_UpgradeDataSync=false but all 5 mapping tables '
                'are empty — content-aware override: cleared by '
                'premapping (force re-sync)'
            )
        elif is_upgrade:
            message = 'CFG_UpgradeDataSync=true: cleared by premapping'
        else:
            message = (
                'CFG_UpgradeDataSync=false: marked Complete by '
                'premapping'
            )

        results = []
        for step, _table, _sequence in MAPPING_STEPS_ORDERED:
            cur.execute(
                f'UPDATE {MAPPING_TABLE_STATE_TABLE_NAME} '
                f'SET Status = ?, Messages = ? WHERE Step = ?',
                (target_status, message, step),
            )
            # Per-row dict carries the structured flags too so the
            # dispatcher's `post_dag_run_details` payload exposes
            # `content_override` / effective-CFG explicitly to the
            # middleware. Previously these were only inferable by
            # string-parsing the `messages` field. The fields are
            # identical across all 4 rows (premapping decisions apply
            # uniformly to every step) — duplication is the cost of
            # keeping each row self-describing for downstream JSON
            # consumers.
            results.append({
                'step': step,
                'status': target_status,
                'messages': message,
                # Raw middleware-supplied conf value (string / bool /
                # None — whatever the integration record carried).
                'cfg_upgrade_data_sync': cfg,
                # `is_upgrade` POST-override — i.e. the value actually
                # used to decide Status. Diverges from
                # `cfg_upgrade_data_sync` only when content_override
                # fired.
                'is_upgrade_effective': is_upgrade,
                # True iff CFG was 'false' but all mapping tables were
                # empty so premapping flipped behaviour to CFG='true'
                # semantics. Operators watching middleware payloads use
                # this to distinguish "trusted external data, skipped
                # sync" from "fresh customer, ran full sync".
                'content_override': overridden,
            })
        conn.commit()

    _log.info(
        "apply_premapping_state: CFG_UpgradeDataSync=%r "
        "(is_upgrade=%s, content_override=%s); "
        "set Status=%r for %d steps",
        cfg, is_upgrade, overridden, target_status, len(results),
    )
    return results


def mark_step_status(step, status, message=''):
    """Update mapping_table_state.Status for one Step.

    Status values: '' | 'Complete' | 'Error' | 'Ready'. Called by:
      - each map_*_dag's `mark_<step>_complete` task on success
      - validate_mappings_dag's summarize task on hard_fail
      - dispatcher's inline Ready handshake
    """
    return _update_mapping_state_status(step, status, message=message)


def check_step_status(step):
    """Return True iff mapping_table_state.Status == 'Complete' for the
    given Step. Used by child DAG skip-gates as the primary check;
    `count_collection_rows` is the secondary defensive check.
    """
    status, _messages = _read_mapping_state_row(step)
    return status == 'Complete'


def mark_all_steps_ready():
    """Bulk set Status='Ready' on every step row. Workato parity with
    `014_503_psa_validate_mapping_tables.recipe.json` line 2192 — the
    final 'integration is ready' handshake. Called by the dispatcher
    inline at the end of the no-errors success path.

    The underlying `UPDATE mapping_table_state SET Status = ?` has no
    WHERE clause, so it flips every row in the table — currently 4
    (firm, employee, account_code, tax_code) but the count
    is data-driven: any row that lands in `mapping_table_state` via
    `seed_mapping_state_rows` is automatically included. Adding a 5th
    mapping step to `MAPPING_STEPS_ORDERED` requires no code change
    here.

    Implemented via ``S3UpdateCollectionOperator``. The operator
    returns ``{'rows_affected': N, ...}`` which we surface as the
    rowcount.
    """
    # pylint: disable=import-outside-toplevel
    from vp_quickbooks_integration.common.tables import (
        MAPPING_TABLE_STATE_TABLE_NAME,
    )

    locator = _resolve_s3_locator()
    result = S3UpdateCollectionOperator(
        task_id='_mark_all_steps_ready',
        integration=locator['integration'],
        customer=locator['customer'],
        integration_type=locator['integration_type'],
        collection_name=MAPPING_TABLE_STATE_TABLE_NAME,
        query=f'UPDATE {MAPPING_TABLE_STATE_TABLE_NAME} SET Status = ?',
        query_params=['Ready'],
    ).execute(locator['context'])
    rowcount = (result or {}).get('rows_affected', 0)
    _log.info("mark_all_steps_ready: set Status='Ready' on %d rows", rowcount)
    return rowcount


# ---------------------------------------------------------------------------
# Child DAG conf builder (used by dispatcher_dag's TriggerDagRunOperators)
# ---------------------------------------------------------------------------

def build_child_dag_conf():
    """
    Build the conf dict passed to each mapping-table child DAG.

    Forwards `connections`, `customerId`, `integrationType`, `region`,
    and the middleware `config` block from the dispatcher's own
    dag_run.conf so child DAGs (employee/firm/account/tax/transaction/
    progress) inherit the same source/target conn IDs, the per-customer
    S3 partition key, the integration_type sub-partition, and every
    per-tenant CFG_* value (CFG_DefaultVendorType,
    CFG_DefaultEmployeeLaborType, CFG_Region, etc.) without any
    per-callable plumbing.

    `region` is resolved CFG-first (`config.CFG_Region`) → top-level
    `region` (legacy passthrough) → `'US'` fallback.
    """
    conf = rail.get_current_context()['dag_run'].conf or {}
    config = conf.get('config') or {}
    return {
        'connections': conf.get('connections') or {},
        'customerId': conf.get('customerId'),
        'company_key': conf.get('company_key'),
        'integrationType': conf.get('integrationType'),
        'region': config.get('CFG_Region') or conf.get('region') or 'US',
        'config': config,
    }


# ---------------------------------------------------------------------------
# Skip-gate helper: count rows in an S3-backed mapping collection
# ---------------------------------------------------------------------------

def count_collection_rows(table_name):
    """
    Return the row count for the given S3 mapping collection.

    Used by child DAGs to skip population when the table is already populated.
    Returns 0 if the collection doesn't exist yet (FileNotFoundError) so the
    first-run case correctly falls through to populate.
    """
    context = rail.get_current_context()
    s3_integration = IntegrationConfig.S3_INTEGRATION_NAME
    s3_customer = IntegrationConfig.get_s3_customer(context)
    s3_integration_type = IntegrationConfig.get_s3_integration_type(context)

    query_op = S3QueryCollectionOperator(
        task_id=f'_count_{table_name}',
        query=f"SELECT COUNT(*) as count FROM {table_name}",
        integration=s3_integration,
        customer=s3_customer,
        integration_type=s3_integration_type,
        mode='single-row',
    )
    try:
        row = query_op.execute(context)
    except FileNotFoundError:
        return 0
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # The S3 file may exist but the specific table may not yet have
        # been created (e.g. a brand-new customer where this child DAG
        # hasn't run its S3CreateCollectionOperator yet). Treat "no such
        # table" as "0 rows" so the skip-gate naturally falls through
        # to the populate path.
        if 'no such table' in str(exc).lower():
            return 0
        raise
    if not row:
        return 0
    if isinstance(row, dict):
        return int(row.get('count', 0) or 0)
    # single-row mode returns a tuple/list in some shapes
    try:
        return int(row[0])
    except (TypeError, ValueError, IndexError):
        return 0


def is_table_populated(table_name):
    """Boolean wrapper around count_collection_rows for IfOperator tests."""
    return count_collection_rows(table_name) > 0


# ---------------------------------------------------------------------------
# Error capture — called by catch_<table>_dag_error tasks
# ---------------------------------------------------------------------------

def capture_dag_error(table_name, customer_id, error_message):
    """
    Capture child-DAG failures for the dispatcher to gather.

    Returns a dict matching vendor_sync's error shape so the dispatcher's
    GatherResultsFromDagRunsOperator + has_sync_errors IfOperator + FailOperator
    chain works without changes. Does NOT raise — the child DAG stays SUCCESS
    and the error info is surfaced one level up.
    """
    cid = (customer_id or '').strip() or '<unknown_customer>'
    msg = (error_message or '').strip() or '<no error message available>'
    return {
        'table': table_name,
        'customerId': cid,
        'error': f"Mapping population for '{table_name}' (customer {cid}) failed: {msg}",
    }


# ===========================================================================
# MAPPING TABLE SCHEMAS
# ===========================================================================
# Table-name + column-list constants live in utils/tables.py — single
# source of truth shared with dispatcher_dag.py's init_mapping_collections.
# The constants are imported at the top of this module for use by the
# SELECT / INSERT / UPDATE statements below.


# ===========================================================================
# MAPPING TABLE SETUP
# ===========================================================================
# The collection tables (mapping + outstanding/state + configuration)
# are created in a single S3 round-trip by
# `dispatcher_dag.init_mapping_collections`, which uses
# `S3CreateMultiTableCollectionOperator`. The operator's per-table
# preserve semantics make it idempotent across re-runs.
#
# `account_type_map` is NOT among these tables: the QBOType → VP type
# lookup is static, read-only product config and ships as the
# `ACCOUNT_TYPE_MAP` Python constant in common/tables.py (mirroring
# PAY_TERMS_MAP / INVOICE_SECTION_CODE_MAP), so there is nothing to seed.


# ===========================================================================
# PER-TENANT LOOKUPS (Airflow Variable-backed; replace bodies when an Airflow
# lookup-table primitive ships — call sites stay unchanged)
# ===========================================================================

def _resolve_cfg_then_variable(cfg_key, variable_name):
    """CFG-first → Variable-fallback resolver for per-tenant defaults.

    Resolution order:
      1. `dag_run.conf['config'][cfg_key]` — the middleware integration
         payload carries CFG_* values per tenant (see
         `IntegrationConfig.get_cfg`). First choice once the middleware
         supplies the key.
      2. `Airflow Variable[variable_name]` — legacy per-instance
         Variable, kept for backwards compatibility during the rollout
         and for keys the middleware payload doesn't ship yet.
      3. `None` when neither source has a value.

    Pulls the active Airflow context via `rail.get_current_context()`;
    falls through to the Variable when no context is available
    (e.g. unit tests outside an Airflow task).
    """
    try:
        context = rail.get_current_context()
    except Exception:  # pylint: disable=broad-exception-caught
        context = None
    if context is not None:
        value = IntegrationConfig.get_cfg(context, cfg_key)
        if value:
            return value
    return Variable.get(variable_name, default_var=None)




# ===========================================================================
# FIRM MAPPING — sync helpers (called by map_firm_dag PythonOperators)
# ===========================================================================

def _extract_qbo_records(rail_result):
    """Normalize a QuickBooks*Operator response into a list of records.

    The QB operators return either:
      - {'success': True/False, 'data': [...], 'error': '...'} (current shape)
      - a raw list (older shape)
      - {'QueryResponse': {<Entity>: [...]}} (raw QB API shape)
    """
    if rail_result is None:
        return []
    if isinstance(rail_result, list):
        return rail_result
    if isinstance(rail_result, dict):
        if rail_result.get('success') is False:
            _log.warning("QBO query failed: %s", rail_result.get('error'))
            return []
        if 'data' in rail_result:
            return rail_result.get('data') or []
        qr = rail_result.get('QueryResponse') or {}
        for entity_key in (
            'Customer', 'Vendor', 'Employee', 'Account', 'TaxCode', 'TaxRate',
        ):
            if entity_key in qr:
                return qr[entity_key] or []
    return []


def _extract_vp_client_id(vp_firm_response):
    """Pull ClientID from a VantagepointFirmOperator response (list or dict)."""
    if isinstance(vp_firm_response, list) and vp_firm_response:
        first = vp_firm_response[0] or {}
        return first.get('ClientID')
    if isinstance(vp_firm_response, dict):
        return vp_firm_response.get('ClientID')
    return None


def _extract_qbo_entity_id(qbo_response, entity_key):
    """Pull Id from a QuickBooks*Operator create response.

    Handles common response shapes (success-dict, QueryResponse, direct dict).
    """
    if not isinstance(qbo_response, dict):
        return None
    if 'data' in qbo_response:
        data = qbo_response.get('data')
        if isinstance(data, list) and data:
            return (data[0] or {}).get('Id')
        if isinstance(data, dict):
            return data.get('Id')
    qr = qbo_response.get('QueryResponse') or {}
    items = qr.get(entity_key) or []
    if items:
        return items[0].get('Id')
    entity_val = qbo_response.get(entity_key)
    if isinstance(entity_val, dict):
        return entity_val.get('Id')
    return None


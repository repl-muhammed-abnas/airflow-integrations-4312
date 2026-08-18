"""
Shared infrastructure for VP Xero mapping_sync.

S3 collection access primitives, per-customer mapping-init Variable gate,
mapping_table_state lifecycle helpers, child DAG conf builder, skip-gate
helpers, error capture, and Xero/VP response normalisers.

Every per-table sync module (`_firm_sync.py`, `_account_sync.py`,
`_tax_code_sync.py`, `_validate.py`) imports from here.
`python_callable_method.py` is a thin shim that re-exports the public surface
for backwards-compat with existing DAG imports.

Employee mapping is out of scope (Q1 = No employee sync), so there is no
`_employee_sync` and no Map Employees step.

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

from vp_xero_integration_v2.mapping_sync.config import IntegrationConfig
from vp_xero_integration_v2.common.python_callable_method import build_customer_variable_key


# Module-level logger for helpers that are called as PythonOperator callbacks.
# Sync helpers grab the task-instance log via
# `rail.get_current_context()['task_instance'].log` for per-task-attempt
# grouping; the module logger covers helpers where the context plumbing isn't
# worth it.
_log = logging.getLogger(__name__)


def _filter_none(body):
    """Drop keys whose value is None. Empty strings are KEPT (recipe parity)."""
    return {k: v for k, v in body.items() if v is not None}


# ---------------------------------------------------------------------------
# Per-customer "mapping initialized" gate
#
# Mapping population is an integration-setup step, not an ongoing sync. Once
# the per-customer Variable `vp_xero_mapping_init_{customerId}_{instance}` is
# 'true', the dispatcher skips all child DAG triggers. The Variable is set by
# mark_mapping_init_complete() at the end of a successful dispatcher run (no
# child-DAG errors); failed runs leave it 'false' so the next run retries.
# ---------------------------------------------------------------------------

# Suffix for the per-customer mapping-init gate; the full key format
# (`vp_xero_<customer>_<suffix>`) lives in common.build_customer_variable_key.
_MAPPING_INIT_SUFFIX = 'mapping_init'

# ---------------------------------------------------------------------------
# Shared S3 collection access primitives
# ---------------------------------------------------------------------------
# Two patterns recurred across this module:
#
#   1. open the artifact via get_or_create_s3_collection_artifact, do sqlite
#      work, let the context manager upload on exit (the WRITEABLE pattern), or
#
#   2. download + decompress directly, do read-only sqlite work, drop the temp
#      dir (the READ-ONLY pattern, used by the validators — bypasses the upload
#      to avoid wasted writes + parallel races).
#
# Both patterns repeat the same "resolve integration/customer/integration_type
# from context" lookup. `_resolve_s3_locator` and `open_mapping_collection`
# consolidate both shapes behind a single context manager.
# ---------------------------------------------------------------------------


def _resolve_s3_locator(context=None):
    """Resolve the (integration, customer, integration_type) triple from the
    active dag_run context. Returns a dict ready to pass into the S3 collection
    operators / artifact helpers.
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
    """Yield a sqlite3 connection to the current dag_run's mapping collection.

    ``read_only=False`` (default): goes through
    ``rail.lib.s3_collection.get_or_create_s3_collection_artifact`` — downloads
    the artifact, yields, and uploads on exit (skipped automatically when the DB
    is unchanged thanks to the RAIL hash short-circuit). Use this for any caller
    that writes to the collection or for atomic check-then-write flows that need
    both reads and writes to run inside a single sqlite session.

    ``read_only=True``: downloads + decompresses directly via ``download_from_s3``
    / ``decompress_file`` and never uploads. Used by the validators; avoids both
    the wasted write and the ETag race when multiple parallel readers touch the
    same key.

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
# mapping_table_state helpers (Workato `populate_mapping_state` parity)
# ---------------------------------------------------------------------------

def seed_mapping_state_rows(instance):
    """Build the seed rows for `mapping_table_state`.

    Consumed by `dispatcher_dag.init_mapping_collections` as the `source`
    argument for the `mapping_table_state` table spec. Workato parity with
    `populate_mapping_state`. Status (col4) is intentionally blank on seed — it
    gets set by `apply_premapping_state` before child DAGs trigger.
    """
    # pylint: disable=import-outside-toplevel
    from vp_xero_integration_v2.common.tables import (
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
    """Map a Workato Step label to the matching Airflow DAG role suffix (the
    second argument to `IntegrationConfig.dag_id`). The account step's DAG role
    is `map_account_code` (matches the child DAG file `map_account_code_dag.py`)
    even though its S3 table is `map_chart_of_accounts`."""
    # pylint: disable=import-outside-toplevel
    from vp_xero_integration_v2.common.tables import (
        MAPPING_STEP_FIRM, MAPPING_STEP_ACCOUNT, MAPPING_STEP_TAX_CODE,
    )
    return {
        MAPPING_STEP_FIRM: 'map_firm',
        MAPPING_STEP_ACCOUNT: 'map_account_code',
        MAPPING_STEP_TAX_CODE: 'map_tax_code',
    }[step]


def _update_mapping_state_status(step, status, message=''):
    """UPDATE mapping_table_state.Status + Messages for one Step.

    Thin wrapper around ``S3UpdateCollectionOperator`` — keeps a stable callable
    surface so callers (`mark_step_status`, `summarize_mapping_validations`)
    don't have to construct operators inline. The operator handles the artifact
    open/upload cycle, including the no-op upload skip when nothing changed.
    """
    # pylint: disable=import-outside-toplevel
    from vp_xero_integration_v2.common.tables import (
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

    Thin wrapper around ``S3QueryCollectionOperator(mode='single-row')`` — same
    pattern as ``count_collection_rows``. Returns ``('', '')`` when the row is
    missing or the table doesn't exist yet.
    """
    # pylint: disable=import-outside-toplevel
    from vp_xero_integration_v2.common.tables import (
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
    """Per-step content-aware premapping state initialization.

    For each of the 3 mapping steps (firm, account, tax_code): if the step's
    own mapping table is empty → Status='' (needs sync); if the table already
    has data → Status='Complete' (already synced, skip).

    CFG_UpgradeDataSync is not applicable to vp_xero_integration_v2. The
    content-driven decision is per-step, so a step whose table is empty always
    triggers a sync regardless of whether sibling tables have data (fixes the
    cross-step blind spot where an accounts sync succeeding while firms failed
    would permanently block a firm re-sync).

    To force a full re-sync: delete the S3 collection and reset the
    per-customer init Variable to 'false'.

    Called as a PythonOperator task in the dispatcher between
    `init_mapping_collections` and `trigger_map_firm`. Per-step skip gates read
    Status via `check_step_status`.
    """
    # pylint: disable=import-outside-toplevel
    from vp_xero_integration_v2.common.tables import (
        MAPPING_STEPS_ORDERED, MAPPING_TABLE_STATE_TABLE_NAME,
    )

    results = []
    with open_mapping_collection() as conn:
        cur = conn.cursor()
        for step, table_name, _sequence in MAPPING_STEPS_ORDERED:
            is_empty = cur.execute(
                f'SELECT 1 FROM {table_name} LIMIT 1'
            ).fetchone() is None
            status = '' if is_empty else 'Complete'
            message = (
                f'premapping: {table_name} is empty — will sync'
                if is_empty else
                f'premapping: {table_name} has data — marked Complete (skip)'
            )
            cur.execute(
                f'UPDATE {MAPPING_TABLE_STATE_TABLE_NAME} '
                f'SET Status = ?, Messages = ? WHERE Step = ?',
                (status, message, step),
            )
            results.append({'step': step, 'status': status, 'messages': message})
        conn.commit()

    _log.info(
        "apply_premapping_state: %s",
        {r['step']: r['status'] for r in results},
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
    """Return True iff mapping_table_state.Status == 'Complete' for the given
    Step. Used by child DAG skip-gates as the primary check;
    `count_collection_rows` is the secondary defensive check.
    """
    status, _messages = _read_mapping_state_row(step)
    return status == 'Complete'


def mark_all_steps_ready():
    """Bulk set Status='Ready' on every step row. Workato parity with the final
    'integration is ready' handshake. Called by the dispatcher inline at the end
    of the no-errors success path.

    The underlying `UPDATE mapping_table_state SET Status = ?` has no WHERE
    clause, so it flips every row in the table — currently 3 (firm, account,
    tax_code) but the count is data-driven: any row that lands in
    `mapping_table_state` via `seed_mapping_state_rows` is automatically
    included. Adding a mapping step to `MAPPING_STEPS_ORDERED` requires no code
    change here.

    Implemented via ``S3UpdateCollectionOperator``. The operator returns
    ``{'rows_affected': N, ...}`` which we surface as the rowcount.
    """
    # pylint: disable=import-outside-toplevel
    from vp_xero_integration_v2.common.tables import (
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

    Forwards `connections`, `customerId`, `integrationType`, `region`, and the
    middleware `config` block from the dispatcher's own dag_run.conf so child
    DAGs (firm/account/tax/validate) inherit the same source/target conn IDs,
    the per-customer S3 partition key, the integration_type sub-partition, and
    every per-tenant CFG_* value without any per-callable plumbing.

    `region` is resolved CFG-first (`config.CFG_Region`) → top-level `region`
    (legacy passthrough) → `'US'` fallback.
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
        # The S3 file may exist but the specific table may not yet have been
        # created (e.g. a brand-new customer where this child DAG hasn't run its
        # S3CreateCollectionOperator yet). Treat "no such table" as "0 rows" so
        # the skip-gate naturally falls through to the populate path.
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

    Returns a dict matching the error shape the dispatcher's
    GatherResultsFromDagRunsOperator + has_sync_errors IfOperator + FailOperator
    chain expects, so the chain works without changes. Does NOT raise — the
    child DAG stays SUCCESS and the error info is surfaced one level up.
    """
    cid = (customer_id or '').strip() or '<unknown_customer>'
    msg = (error_message or '').strip() or '<no error message available>'
    return {
        'table': table_name,
        'customerId': cid,
        'error': f"Mapping population for '{table_name}' (customer {cid}) failed: {msg}",
    }


# ===========================================================================
# MAPPING TABLE SETUP
# ===========================================================================
# The collection tables (mapping + state) are created in a single S3 round-trip
# by `dispatcher_dag.init_mapping_collections`, which uses
# `S3CreateMultiTableCollectionOperator`. The operator's per-table preserve
# semantics make it idempotent across re-runs.
#
# Unlike the QBO package (where the account-type lookup is a static Python
# constant), `map_account_type` IS one of these tables for Xero: it is created
# AND seeded by `init_mapping_collections` from
# `common.tables.ACCOUNT_TYPE_SEED_ROWS` (Q7 = A: data-driven seeded collection,
# Workato parity). `_account_sync` reads it as a lookup at sync time.


# ===========================================================================
# PER-TENANT LOOKUPS (Airflow Variable-backed; replace bodies when an Airflow
# lookup-table primitive ships — call sites stay unchanged)
# ===========================================================================

def _resolve_cfg_then_variable(cfg_key, variable_name):
    """CFG-first → Variable-fallback resolver for per-tenant defaults.

    Resolution order:
      1. `dag_run.conf['config'][cfg_key]` — the middleware integration payload
         carries CFG_* values per tenant (see `IntegrationConfig.get_cfg`).
         First choice once the middleware supplies the key.
      2. `Airflow Variable[variable_name]` — legacy per-instance Variable, kept
         for backwards compatibility during the rollout and for keys the
         middleware payload doesn't ship yet.
      3. `None` when neither source has a value.

    Pulls the active Airflow context via `rail.get_current_context()`; falls
    through to the Variable when no context is available (e.g. unit tests
    outside an Airflow task).
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
# XERO / VP RESPONSE NORMALISERS (called by the per-table sync engines)
# ===========================================================================

def _extract_xero_records(rail_result):
    """Normalize a Xero*Operator response into a list of records.

    The Xero operators return either:
      - {'success': True/False, 'data': [...], 'error': '...'} (typed-operator
        shape from XeroBaseOperator._format_xero_response)
      - a raw list (older shape)
      - {'<Resource>': [...]} (raw XeroAPIOperator envelope, e.g. {'Contacts': [...]})
    """
    if rail_result is None:
        return []
    if isinstance(rail_result, list):
        return rail_result
    if isinstance(rail_result, dict):
        if rail_result.get('success') is False:
            _log.warning("Xero query failed: %s", rail_result.get('error'))
            return []
        if 'data' in rail_result:
            data = rail_result.get('data')
            if isinstance(data, list):
                return data
            return [data] if data else []
        for entity_key in (
            'Contacts', 'Accounts', 'TaxRates', 'Invoices', 'CreditNotes',
            'ManualJournals', 'Payments', 'Currencies',
        ):
            if entity_key in rail_result:
                return rail_result[entity_key] or []
    return []


def _extract_vp_client_id(vp_firm_response):
    """Pull ClientID from a VantagepointFirmOperator response (list or dict)."""
    if isinstance(vp_firm_response, list) and vp_firm_response:
        first = vp_firm_response[0] or {}
        return first.get('ClientID')
    if isinstance(vp_firm_response, dict):
        return vp_firm_response.get('ClientID')
    return None


def _extract_xero_entity_id(xero_response, id_field='ContactID'):
    """Pull the Xero entity id from a Xero*Operator create/query response.

    `id_field` is the Xero id attribute for the entity (e.g. 'ContactID',
    'AccountID', 'InvoiceID'). Reuses `_extract_xero_records` to normalise the
    response shape, then returns the id of the first record.
    """
    records = _extract_xero_records(xero_response)
    if records:
        first = records[0] or {}
        return first.get(id_field)
    return None

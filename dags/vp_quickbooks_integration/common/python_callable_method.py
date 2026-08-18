"""Shared Python callable helpers for vp_quickbooks_integration.

Home for methods reused across the integration DAGs (vendor_sync, employee_sync,
employee_sync_upsert, journal_entry_sync, chart_of_accounts_sync, ap_voucher,
...). Lives in `common` — alongside `common.tables` and `common.config`, which
already deploy + import cross-integration — so a fix is made in ONE place
instead of being copied into every integration's utils module.

Currently provides the SQLite-in-S3 collection-access surface (locator,
single-row read, multi-row read via json_group_array, and the
INSERT/UPDATE/DELETE write surface). Add other cross-integration helpers below
as they're identified.

Collection access notes: every consumer reads/writes the per-customer
collection that the `mapping_sync` integration owns, so `integration_type` is
pinned to 'mapping_sync' for all of them. The only per-integration variable is
the READ operator's `task_id` — purely cosmetic (these operators are executed
directly via `.execute()`, not wired into a DAG) — so callers pass their own
label via `read_task_id`.
"""
import json
import logging
import re
from datetime import datetime, timezone

import rail
from airflow.models import Variable
from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig

logger = logging.getLogger(__name__)

# The lookup / state tables this family of integrations consumes are created and
# populated by `mapping_sync` under its own integration_type partition, so every
# reader/writer pins integration_type to this value (NOT the consuming
# integration's own conf integrationType) to hit the same S3 object.
MAPPING_COLLECTION_INTEGRATION_TYPE = 'mapping_sync'

# Default task_id for the read operator when a caller doesn't pass its own.
_DEFAULT_READ_TASK_ID = '_read_mapping_collection'

# Single source of truth for the per-customer Airflow Variable key FORMAT.
# Every per-customer key across these integrations is `vp_qbo_<customer>_<...>`,
# so the prefix lives here once — a format change is a one-line edit, not an
# edit in every integration's config/dispatcher.
_VARIABLE_KEY_PREFIX = 'vp_qbo'


def collection_integration(context):
    """The (integration, customer, integration_type) triple that locates the
    mapping_sync-owned collection for the current tenant. integration_type is
    pinned to 'mapping_sync' regardless of this integration's own conf
    integrationType."""
    return (
        IntegrationConfig.S3_INTEGRATION_NAME,
        IntegrationConfig.get_s3_customer(context),
        MAPPING_COLLECTION_INTEGRATION_TYPE,
    )


def collection_single_row(query, params, context=None,
                          read_task_id=_DEFAULT_READ_TASK_ID):
    """Run a read query returning 0 or 1 rows through S3QueryCollectionOperator
    (single-row mode). Returns the row (dict) or None. A missing collection /
    table is treated as "no row" (None)."""
    context = context or rail.get_current_context()
    integration, customer, integration_type = collection_integration(context)
    op = rail.S3QueryCollectionOperator(
        task_id=read_task_id,
        query=query,
        query_params=params,
        integration=integration,
        customer=customer,
        integration_type=integration_type,
        mode='single-row',
    )
    try:
        return op.execute(context)
    except FileNotFoundError:
        logger.warning("Mapping collection not found yet for this tenant.")
        return None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if 'no such table' in str(exc).lower():
            logger.warning("Collection table missing: %s", exc)
            return None
        raise


def collection_rows(table, columns, where_sql, params, context=None,
                    read_task_id=_DEFAULT_READ_TASK_ID):
    """Read multiple rows from a mapping_sync collection through a SINGLE
    S3QueryCollectionOperator call.

    S3QueryCollectionOperator only returns rows in 'single-row' mode, so we pack
    every matching row into one JSON array via json_group_array(json_object(...))
    and unpack it in Python. Column names come from common.tables constants so
    identifiers can't drift. Each row includes its sqlite rowid as '_rowid'.
    Returns list[dict] ([] if the collection/table is missing or nothing
    matches)."""
    context = context or rail.get_current_context()
    pairs = "'_rowid', rowid, " + ", ".join(f"'{c}', {c}" for c in columns)
    query = (
        f"SELECT json_group_array(json_object({pairs})) AS rows "
        f"FROM {table} WHERE {where_sql}"
    )
    row = collection_single_row(query, params, context, read_task_id)
    if not row:
        return []
    raw = row.get('rows') if isinstance(row, dict) else (row[0] if row else None)
    if not raw:
        return []
    try:
        return json.loads(raw) or []
    except (TypeError, ValueError):
        logger.warning("Could not parse collection rows JSON for %s", table)
        return []


def collection_update(collection_name, query, params, context=None):
    """Run an INSERT/UPDATE/DELETE against the mapping_sync collection via
    S3UpdateCollectionOperator (the canonical lock surface). Returns the
    operator result dict."""
    context = context or rail.get_current_context()
    integration, customer, integration_type = collection_integration(context)
    op = rail.S3UpdateCollectionOperator(
        task_id=f'_update_{collection_name}',
        integration=integration,
        customer=customer,
        integration_type=integration_type,
        collection_name=collection_name,
        query=query,
        query_params=params,
    )
    return op.execute(context)


def collection_upsert(collection_name, key_columns, data_columns,
                      context=None, upsert_mode='REPLACE'):
    """Upsert ONE row into the mapping_sync collection via
    S3UpsertCollectionOperator (atomic ``INSERT ... ON CONFLICT(key_columns) DO
    UPDATE`` — the canonical lock surface, one S3 download/modify/upload/lock
    cycle). Replaces the old DELETE-then-INSERT idiom (two separate S3 commits,
    non-atomic) now that the map tables carry UNIQUE indexes.

    The target table MUST have a UNIQUE/PRIMARY KEY index covering EXACTLY
    `key_columns` (created by mapping_sync's `init_mapping_collections` from the
    table's `unique_columns` spec), or SQLite raises on the ON CONFLICT clause.
    `data_columns` is the full row dict (must include the key columns). Returns
    the operator result dict."""
    context = context or rail.get_current_context()
    integration, customer, integration_type = collection_integration(context)
    op = rail.S3UpsertCollectionOperator(
        task_id=f'_upsert_{collection_name}',
        integration=integration,
        customer=customer,
        integration_type=integration_type,
        collection_name=collection_name,
        key_columns=key_columns,
        data_columns=data_columns,
        upsert_mode=upsert_mode,
    )
    return op.execute(context)


def collection_operations(collection_name, operations, context=None, atomic=True):
    """Run a heterogeneous, ordered list of statements against the mapping_sync
    collection in ONE S3 download/modify/upload/lock cycle via
    S3UpdateCollectionOperator's operations mode. ``operations`` is a list of
    ``{'query': str, 'query_params': list}`` dicts executed in order.

    ``atomic=True`` (default) makes the batch all-or-nothing: the first failure
    aborts the batch before commit, rolls the SQLite transaction back, and
    leaves S3 untouched — use for a DELETE-then-INSERT (one-to-many replace)
    that must not leave rows deleted-but-not-reinserted. Returns the operator
    result dict."""
    context = context or rail.get_current_context()
    integration, customer, integration_type = collection_integration(context)
    op = rail.S3UpdateCollectionOperator(
        task_id=f'_ops_{collection_name}',
        integration=integration,
        customer=customer,
        integration_type=integration_type,
        collection_name=collection_name,
        operations=operations,
        atomic=atomic,
    )
    return op.execute(context)


# ---------------------------------------------------------------------------
# Vantagepoint response normalization
# ---------------------------------------------------------------------------
def unwrap_vp_response(raw, strict=False):
    """Normalize the assorted shapes a Vantagepoint operator may return into a
    list of records.

    `strict=True` raises ValueError on an unrecognised dict envelope (or a
    non-list/non-dict value) so "a shape we don't know" is never silently
    treated as "no records"; `strict=False` logs and returns []. (Each
    integration previously kept a private copy of this helper.)
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ('rows', 'Body', 'body', 'array', 'data'):
            value = raw.get(key)
            if isinstance(value, list):
                return value
        message = (
            f"Unrecognised VP response shape: dict keys={sorted(raw.keys())}"
        )
        if strict:
            raise ValueError(message)
        logger.warning("%s", message)
        return []
    message = (
        f"Unrecognised VP response shape: {type(raw).__name__} "
        f"(expected list or dict)"
    )
    if strict:
        raise ValueError(message)
    logger.warning("%s", message)
    return []


# ---------------------------------------------------------------------------
# Lookup-Variable reader (pluggable)
# ---------------------------------------------------------------------------
def read_lookup_variable(variable_key, default=None):
    """Read a JSON-or-scalar Airflow Variable; return `default` when absent.

    Used for per-instance config Variables (e.g. default labor type / vendor
    type). JSON-decodes the value when possible, else returns the raw string.
    """
    raw = Variable.get(variable_key, default_var=None)
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw


# ---------------------------------------------------------------------------
# Dispatcher error gate (3-DAG integrations)
# ---------------------------------------------------------------------------
def has_sync_errors_method():
    """IfOperator test (dispatcher): did any processor child report an error?

    Reads the dispatcher's `gather_processor_dag_errors` XCom (the conventional
    task_id across the 3-DAG integrations)."""
    return len(rail.result('gather_processor_dag_errors') or []) > 0


# ---------------------------------------------------------------------------
# Per-customer watermark (last-sync-time) helpers
#
# The functions are integration-agnostic — the per-integration values
# (`watermark_variable_key_template`, `initial_sync_time`) are passed in by the
# caller from its own config.
# ---------------------------------------------------------------------------
_CUSTOMER_ID_SAFE_RE = re.compile(r'[^A-Za-z0-9_-]')


def sanitize_customer_id(customer_id):
    """Strip Airflow-Variable-unsafe chars; fall back to 'default' when empty.

    `customerId` arrives from middleware and can contain characters Airflow's
    Variable storage refuses (spaces, slashes, dots). This normalizes to
    `[A-Za-z0-9_-]` so the resulting Variable key is round-trippable.
    """
    if not customer_id:
        return 'default'
    cleaned = _CUSTOMER_ID_SAFE_RE.sub('_', str(customer_id))
    return cleaned or 'default'


def build_watermark_variable_key(template, instance, customer_id):
    """Render a per-(instance, customer) watermark Variable key.

    `template` is integration-specific (e.g.
    `'vp_qbo_{customer_id}_<integrationType>_sync_last_run'`) and must contain the
    `{customer_id}` placeholder; the `{instance}` placeholder is optional
    (always supplied here, ignored by `str.format` when absent). customer_id
    is sanitized before substitution.
    """
    return template.format(
        instance=instance,
        customer_id=sanitize_customer_id(customer_id),
    )


def build_customer_variable_key(customer_id, suffix):
    """Canonical per-customer Airflow Variable key: `vp_qbo_<customer>_<suffix>`.

    Single source of truth for the per-customer key format. `suffix` is the
    integration-specific tail, e.g. `'vendor_sync_last_run'`, `'mapping_init'`,
    `'mapping_sync_last_run'`. customer_id is sanitized before substitution.
    """
    return f'{_VARIABLE_KEY_PREFIX}_{sanitize_customer_id(customer_id)}_{suffix}'


def watermark_key_template(integration, variable_name='last_run'):
    """`str.format` template for a per-customer, per-integration Variable key.

    Defaults to the `'last_run'` watermark, e.g.
    `watermark_key_template('vendor_sync')` ->
    `'vp_qbo_{customer_id}_vendor_sync_last_run'`. Pass `variable_name` for any
    other key following the `vp_qbo_{customer_id}_<integration>_<variable_name>`
    convention, e.g. `watermark_key_template('vendor_sync', 'high_watermark')`
    -> `'vp_qbo_{customer_id}_vendor_sync_high_watermark'`.

    The `{customer_id}` placeholder is filled + sanitized by
    `build_watermark_variable_key` (or any `.format(customer_id=...)` call), so
    it feeds the template-based `prepare_sync_timestamps` /
    `update_last_sync_time` helpers unchanged. Centralizing the format here
    means a token reorder is one edit instead of one per integration config.

    If you already have the customer_id in hand at runtime, prefer
    `build_customer_variable_key(customer_id, f'{integration}_{variable_name}')`
    — it resolves + sanitizes in one step without a template round-trip.
    """
    return (
        f'{_VARIABLE_KEY_PREFIX}_{{customer_id}}_{integration}_{variable_name}'
    )


def utc_now_iso():
    """ISO-8601 millisecond UTC timestamp with 'Z' suffix."""
    return (
        datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        + 'Z'
    )


def prepare_sync_timestamps(instance, template, initial_sync_time):
    """Capture last + current sync time for a per-customer watermark.

    `template` is the integration-specific watermark Variable key template
    (must contain `{customer_id}`). `initial_sync_time` is the
    first-run backstop used when no Variable exists yet for this
    (instance, customer) pair.

    Returns the dict shape every sibling integration relies on:
        {'last_sync_time': <iso>, 'current_sync_time': <iso>}
    """
    customer_id = (
        rail.get_current_context()['dag_run'].conf.get('customerId')
    )
    key = build_watermark_variable_key(template, instance, customer_id)
    current_time = utc_now_iso()
    try:
        last_sync_time = Variable.get(key)
        logger.info(
            "Retrieved last sync time from Variable '%s': %s",
            key, last_sync_time
        )
    except KeyError:
        last_sync_time = initial_sync_time
        logger.info(
            "Variable '%s' not found, using initial sync time: %s",
            key, last_sync_time
        )
    return {
        'last_sync_time': last_sync_time,
        'current_sync_time': current_time,
    }


def update_last_sync_time(instance, template):
    """Persist `current_sync_time` into the per-customer watermark Variable.

    Reads the XCom from the upstream task with id `prepare_sync_timestamps`
    (the conventional task_id used across sibling integrations). Skips the write
    when that XCom is missing or has no `current_sync_time` — we never advance
    the watermark past records we haven't actually processed.
    """
    timestamps = rail.result('prepare_sync_timestamps')
    if not isinstance(timestamps, dict) or not timestamps.get(
        'current_sync_time'
    ):
        logger.warning(
            "prepare_sync_timestamps did not produce a current_sync_time "
            "(skipped or failed); leaving watermark Variable unchanged."
        )
        return None
    customer_id = (
        rail.get_current_context()['dag_run'].conf.get('customerId')
    )
    key = build_watermark_variable_key(template, instance, customer_id)
    current_time = timestamps['current_sync_time']
    Variable.set(key, current_time)
    logger.info(
        "Updated last sync time Variable '%s' to: %s", key, current_time
    )
    return current_time

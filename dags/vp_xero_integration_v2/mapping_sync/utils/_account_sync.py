"""
Account code mapping sync (Xero Chart of Accounts → VP Chart of Accounts).

Operator-driven port of the Workato recipes `014_501_psa_synch_accounts`
(orchestrator) + `014_501_psa_sync_accounts` (worker), with the `Map Accounts`
seeder folded in (Option A). See reverse-engineering docs 02-synch-accounts.md +
06-lookup-table-seeding.md.

The DAG stages three run-local collections (xero_accounts, vp_accounts,
chart_of_accounts_map) and runs the recipe's compile JOIN
(`COMPILE_ACCOUNT_CODES_SQL`) producing per-row decisions; this module supplies
the collection sources, the SQL, the foreach engine, and a scoped
orphan-deactivation pass.

Xero specifics vs the QBO port:
  - Source op is `XeroAccountOperator` (Code/Name/Type/Status/AccountID).
  - The Xero-Type → VP-type translation reads the SEEDED `map_account_type`
    S3 collection (Q7 = A) into a dict, rather than a static Python constant.
  - Unmapped Xero types are SURFACED in the map row's Messages (not silently
    dropped as the Workato INNER JOIN did — Q9 / Q-S3).
  - Natural key is XeroID (single-column UNIQUE) → idempotent upsert.
  - Orphan deactivation is SCOPED (Q6 = A): only VP accounts previously
    Xero-sourced (present in map_chart_of_accounts) whose XeroID is no longer in
    the current Xero set are deactivated — manually-created VP accounts are left
    untouched.

Public surface (re-exported via `python_callable_method.py`):
    sync_xero_accounts_to_vp                 — compile foreach (PythonOperator)
    build_xero_accounts_staging              — CreateCollectionOperator source
    prepare_vp_accounts_staging              — CreateCollectionOperator source
    read_chart_of_accounts_map_for_staging   — CreateCollectionOperator source
    COMPILE_ACCOUNT_CODES_SQL                — QueryCollectionOperator query
"""
import rail

from vp_xero_integration_v2.mapping_sync.utils._shared import (
    _extract_xero_records,
    _filter_none,
)
from vp_xero_integration_v2.common.python_callable_method import unwrap_vp_response
from vp_xero_integration_v2.common.tables import (
    MAP_ACCOUNT_TYPE_TABLE_NAME,
    MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
    MAP_CHART_OF_ACCOUNTS_UNIQUE_COLUMNS,
)
from vp_xero_integration_v2.mapping_sync.config import IntegrationConfig


# ===========================================================================
# Run-local staging collections + compile SQL (recipe steps 3-14)
# ===========================================================================

XERO_ACCOUNTS_COLLECTION = 'xero_accounts'
VP_ACCOUNTS_COLLECTION = 'vp_accounts'
CHART_OF_ACCOUNTS_MAP_COLLECTION = 'chart_of_accounts_map'
COMPILED_ACCOUNT_CODES_COLLECTION = 'compiled_account_codes'

XERO_ACCOUNTS_STAGING_COLUMNS = ['Code', 'Name', 'Type', 'Status', 'AccountID']
VP_ACCOUNTS_STAGING_COLUMNS = ['Account', 'Name', 'Type']
CHART_OF_ACCOUNTS_MAP_STAGING_COLUMNS = [
    'XeroCode', 'XeroName', 'XeroType', 'VantagepointCode',
    'VantagepointName', 'VantagepointType', 'XeroID', 'EntryID',
]

# Recipe step 14 — Xero-primary compile JOIN. Matches Xero accounts to existing
# VP accounts by Code = Account, and to the existing map by AccountID = XeroID OR
# Account = VantagepointCode. BANK accounts are excluded (parity). The
# Xero-Type → VP-type translation is resolved in Python from the seeded
# map_account_type collection (NOT a SQL join) so unmapped types can be surfaced
# rather than dropped by an INNER JOIN.
COMPILE_ACCOUNT_CODES_SQL = (
    "SELECT "
    "  xa.Code AS XeroCode, xa.Name AS XeroName, xa.Type AS XeroType, "
    "  xa.AccountID AS XeroID, xa.Status AS XeroStatus, "
    "  va.Account AS VantagepointCode, va.Name AS VantagepointName, "
    "  va.Type AS VantagepointType, "
    "  ma.XeroID AS MappedXeroID, ma.VantagepointCode AS MappedVantagepointCode, "
    "  ma.EntryID AS EntryID "
    "FROM xero_accounts xa "
    "LEFT JOIN vp_accounts va ON xa.Code = va.Account "
    "LEFT JOIN chart_of_accounts_map ma "
    "  ON xa.AccountID = ma.XeroID OR va.Account = ma.VantagepointCode "
    "WHERE UPPER(IFNULL(xa.Type, '')) != 'BANK' "
    "ORDER BY xa.Name"
)

# VP Chart-of-Accounts `Account` column limit fallback when System Formats can't
# be read (mirrors the QBO port's observed default).
_VP_ACCOUNT_CODE_DEFAULT_MAX_LEN = 13
_MAX_ACCOUNT_LENGTH_CANDIDATE_KEYS = (
    'AccountLength', 'AcctLength', 'MaxLength', 'Length', 'Size', 'FieldLength',
)
# VP truncates account Name to 39 chars.
_VP_ACCOUNT_NAME_MAX_LEN = 39


def _resolve_vp_account_max_len(context):  # pylint: disable=unused-argument
    """VP Account-code max length from the `get_system_formats` task result
    (recipe AccountLength), else the observed default."""
    try:
        result = rail.result('get_system_formats')
    except Exception:  # pylint: disable=broad-exception-caught
        return _VP_ACCOUNT_CODE_DEFAULT_MAX_LEN

    if isinstance(result, dict):
        formats = result.get('formats')
        rows = formats if isinstance(formats, list) else [result]
    elif isinstance(result, list):
        rows = result
    else:
        rows = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in _MAX_ACCOUNT_LENGTH_CANDIDATE_KEYS:
            value = row.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text.isdigit() and int(text) > 0:
                return int(text)
    return _VP_ACCOUNT_CODE_DEFAULT_MAX_LEN


def build_xero_accounts_staging(**_context):
    """CreateCollectionOperator source for `xero_accounts`."""
    return [
        {
            'Code': a.get('Code') or '',
            'Name': a.get('Name') or '',
            'Type': a.get('Type') or '',
            'Status': a.get('Status') or '',
            'AccountID': str(a.get('AccountID') or ''),
        }
        for a in _extract_xero_records(rail.result('fetch_xero_accounts'))
        if a.get('AccountID')
    ]


def prepare_vp_accounts_staging(**_context):
    """CreateCollectionOperator source for `vp_accounts`."""
    records = unwrap_vp_response(rail.result('fetch_vp_accounts'))
    return [
        {
            'Account': r.get('Account') or '',
            'Name': r.get('Name') or '',
            'Type': r.get('Type') or '',
        }
        for r in records
        if isinstance(r, dict) and r.get('Account')
    ]


def read_chart_of_accounts_map_for_staging(**_context):
    """CreateCollectionOperator source for `chart_of_accounts_map`.

    Copies the persistent S3 map_chart_of_accounts rows (plus the sqlite rowid as
    EntryID) into the run-local collection so the compile JOIN can read the
    existing mapping. Empty on a first sync.
    """
    import sqlite3  # pylint: disable=import-outside-toplevel
    import rail.lib.s3_collection  # pylint: disable=import-outside-toplevel

    context = rail.get_current_context()
    s3_integration = IntegrationConfig.S3_INTEGRATION_NAME
    s3_customer = IntegrationConfig.get_s3_customer(context)
    s3_integration_type = IntegrationConfig.get_s3_integration_type(context)
    s3_artifact_name = rail.lib.s3_collection.get_s3_collection_artifact_name(
        context, s3_integration, s3_customer, s3_integration_type
    )
    rows = []
    with rail.lib.s3_collection.get_or_create_s3_collection_artifact(
        s3_artifact_name, s3_integration, s3_customer, context,
        integration_type=s3_integration_type, use_lock=False,
    ) as artifact:
        with sqlite3.connect(artifact.local_filename) as conn:
            cur = conn.cursor()
            cur.execute(
                f'SELECT rowid, XeroCode, XeroName, XeroType, VantagepointCode, '
                f'VantagepointName, VantagepointType, XeroID '
                f'FROM {MAP_CHART_OF_ACCOUNTS_TABLE_NAME}'
            )
            for (entry_id, xero_code, xero_name, xero_type, vp_code, vp_name,
                 vp_type, xero_id) in cur.fetchall():
                rows.append({
                    'XeroCode': xero_code, 'XeroName': xero_name,
                    'XeroType': xero_type, 'VantagepointCode': vp_code,
                    'VantagepointName': vp_name, 'VantagepointType': vp_type,
                    'XeroID': xero_id, 'EntryID': entry_id,
                })
    return rows


# ===========================================================================
# Seeded type lookup + body builders
# ===========================================================================

def _load_account_type_index(context):
    """Read the seeded map_account_type S3 collection into a dict
    {XeroType.upper() -> VantagepointCode}. Lock-free read (reference data)."""
    import sqlite3  # pylint: disable=import-outside-toplevel
    import rail.lib.s3_collection  # pylint: disable=import-outside-toplevel

    s3_integration = IntegrationConfig.S3_INTEGRATION_NAME
    s3_customer = IntegrationConfig.get_s3_customer(context)
    s3_integration_type = IntegrationConfig.get_s3_integration_type(context)
    s3_artifact_name = rail.lib.s3_collection.get_s3_collection_artifact_name(
        context, s3_integration, s3_customer, s3_integration_type
    )
    index = {}
    with rail.lib.s3_collection.get_or_create_s3_collection_artifact(
        s3_artifact_name, s3_integration, s3_customer, context,
        integration_type=s3_integration_type, use_lock=False,
    ) as artifact:
        with sqlite3.connect(artifact.local_filename) as conn:
            cur = conn.cursor()
            cur.execute(
                f'SELECT XeroType, VantagepointCode '
                f'FROM {MAP_ACCOUNT_TYPE_TABLE_NAME}'
            )
            for xero_type, vp_code in cur.fetchall():
                if xero_type:
                    index[str(xero_type).strip().upper()] = vp_code
    return index


def _truncate_name(name):
    """VP truncates account Name to 39 chars (recipe `XeroName.slice(0,39)`)."""
    return (name or '')[:_VP_ACCOUNT_NAME_MAX_LEN]


def build_vp_account_create_body(xero_code, xero_name, vp_type_code):
    """POST /Accounts body. Account=XeroCode, Name truncated to 39, Type=mapped
    VP type, Status=A, Detail=1; the balancing-account + QBOAccountID columns are
    sent blank (VP's schema layer requires column presence — empty strings, not
    omitted keys)."""
    return _filter_none({
        'Account': xero_code,
        'Name': _truncate_name(xero_name),
        'Type': vp_type_code,
        'Status': 'A',
        'CashBasisAccount': '',
        'UnrealizedLossAccount': '',
        'UnrealizedGainAccount': '',
        'CashBasisRevaluation': '',
        'QBOAccountID': '',
        'Detail': '1',
    })


def build_vp_account_update_body(xero_name, vp_type_code, xero_status):
    """PUT /Accounts/{Account} body. Same fields as create minus Account (the
    URL carries it). Status follows the Xero account status (ACTIVE → 'A', else
    'I')."""
    status = 'A' if str(xero_status or '').strip().upper() == 'ACTIVE' else 'I'
    return _filter_none({
        'Name': _truncate_name(xero_name),
        'Type': vp_type_code,
        'Status': status,
        'CashBasisAccount': '',
        'UnrealizedLossAccount': '',
        'UnrealizedGainAccount': '',
        'CashBasisRevaluation': '',
        'QBOAccountID': '',
        'Detail': '1',
    })


def _build_map_row(*, xero_code, xero_name, xero_type, vp_code, vp_name,
                   vp_type, xero_id, messages):
    """Assemble one map_chart_of_accounts row dict (all 8 columns). Natural key
    is XeroID (MAP_CHART_OF_ACCOUNTS_UNIQUE_COLUMNS)."""
    return {
        'XeroCode': xero_code,
        'XeroName': xero_name,
        'XeroType': xero_type,
        'VantagepointCode': vp_code,
        'VantagepointName': vp_name,
        'VantagepointType': vp_type,
        'XeroID': xero_id,
        'Messages': messages,
    }


def _read_compiled_account_codes(context):
    """Read the run-local `compiled_account_codes` collection (compile output)."""
    import sqlite3  # pylint: disable=import-outside-toplevel
    import rail.lib.collection  # pylint: disable=import-outside-toplevel

    artifact_name = rail.lib.collection.get_collection_artifact_name(context)
    rows = []
    with rail.lib.collection.get_or_create_collection_artifact(
        artifact_name, context
    ) as artifact:
        with sqlite3.connect(artifact.local_filename) as conn:
            cur = conn.cursor()
            cur.execute(f'SELECT * FROM {COMPILED_ACCOUNT_CODES_COLLECTION}')
            columns = [d[0] for d in cur.description]
            for values in cur.fetchall():
                rows.append(dict(zip(columns, values)))
    return rows


def _read_existing_map_rows(context):
    """Lock-free read of all persistent map_chart_of_accounts rows (used for the
    scoped orphan-deactivation pass)."""
    import sqlite3  # pylint: disable=import-outside-toplevel
    import rail.lib.s3_collection  # pylint: disable=import-outside-toplevel

    s3_integration = IntegrationConfig.S3_INTEGRATION_NAME
    s3_customer = IntegrationConfig.get_s3_customer(context)
    s3_integration_type = IntegrationConfig.get_s3_integration_type(context)
    s3_artifact_name = rail.lib.s3_collection.get_s3_collection_artifact_name(
        context, s3_integration, s3_customer, s3_integration_type
    )
    rows = []
    with rail.lib.s3_collection.get_or_create_s3_collection_artifact(
        s3_artifact_name, s3_integration, s3_customer, context,
        integration_type=s3_integration_type, use_lock=False,
    ) as artifact:
        with sqlite3.connect(artifact.local_filename) as conn:
            cur = conn.cursor()
            cur.execute(
                f'SELECT XeroCode, XeroName, XeroType, VantagepointCode, '
                f'VantagepointName, VantagepointType, XeroID, Messages '
                f'FROM {MAP_CHART_OF_ACCOUNTS_TABLE_NAME}'
            )
            cols = ['XeroCode', 'XeroName', 'XeroType', 'VantagepointCode',
                    'VantagepointName', 'VantagepointType', 'XeroID', 'Messages']
            for values in cur.fetchall():
                rows.append(dict(zip(cols, values)))
    return rows


def sync_xero_accounts_to_vp(instance):  # pylint: disable=unused-argument,too-many-locals,too-many-branches,too-many-statements
    """Forward sync (Xero Chart of Accounts → VP Chart of Accounts).

    The compile JOIN (`compiled_account_codes`) is materialized upstream by the
    DAG. For each compiled row:
      - matched an existing VP account by Code (and Name equal) → record it (no create);
      - no VP match + Xero ACTIVE + a mapped VP type → create the VP account;
      - already mapped, VP name drifted → PUT the VP name/status;
      - unmapped Xero type → record the row with a Messages note (NOT dropped).

    Then a scoped orphan pass deactivates VP accounts that were previously
    Xero-sourced (in the map) but whose XeroID is no longer in the current Xero
    set (Q6 = A — manually-created VP accounts are untouched).

    All map rows are written in a single batched upsert keyed on XeroID. Raises
    RuntimeError at the end if any per-record failure occurred.
    """
    from rail import (  # pylint: disable=import-outside-toplevel
        S3UpsertCollectionOperator,
        VantagepointChartOfAccountsOperator,
    )

    context = rail.get_current_context()
    log = context['task_instance'].log

    conn_ids = IntegrationConfig.get_conn_ids(context)
    vp_conn_id = conn_ids['vp_conn_id']

    compiled_rows = _read_compiled_account_codes(context)
    log.info("Read %d compiled account rows", len(compiled_rows))

    max_account_len = _resolve_vp_account_max_len(context)
    type_index = _load_account_type_index(context)
    log.info("VP max account-code length: %d; %d account-type mappings",
             max_account_len, len(type_index))

    current_xero_ids = {
        str(a.get('AccountID'))
        for a in _extract_xero_records(rail.result('fetch_xero_accounts'))
        if a.get('AccountID')
    }

    s3_integration = IntegrationConfig.S3_INTEGRATION_NAME
    s3_customer = IntegrationConfig.get_s3_customer(context)
    s3_integration_type = IntegrationConfig.get_s3_integration_type(context)

    summary = {
        'created': 0, 'updated': 0, 'matched_existing': 0,
        'unmapped_type': 0, 'skipped_account_code_too_long': 0,
        'deactivated_orphans': 0, 'errors': [],
    }
    map_rows = []

    def _vp_type(xero_type):
        return type_index.get(str(xero_type or '').strip().upper())

    # ---- Phase 1: per compiled row — match / create / update (no S3 lock) ----
    for row in compiled_rows:
        xero_id = row.get('XeroID')
        if not xero_id:
            continue
        xero_code = row.get('XeroCode') or ''
        xero_name = row.get('XeroName') or ''
        xero_type = row.get('XeroType') or ''
        xero_status = row.get('XeroStatus') or ''
        vp_code = row.get('VantagepointCode') or ''
        vp_name = row.get('VantagepointName') or ''
        vp_type = row.get('VantagepointType') or ''
        mapped_vp = row.get('MappedVantagepointCode') or ''

        out_code, out_name, out_type, messages = '', '', '', ''
        type_code = _vp_type(xero_type)

        try:
            if not mapped_vp:
                if vp_code and _truncate_name(xero_name) == vp_name:
                    # Matched an existing VP account by Code (+ Name) → record it.
                    out_code, out_name, out_type = vp_code, vp_name, vp_type
                    summary['matched_existing'] += 1
                elif not vp_code and str(xero_status).strip().upper() == 'ACTIVE':
                    # No VP match → create (only when the type maps + code fits).
                    if type_code is None:
                        # Surface unmapped type — do NOT silently drop (Q9/Q-S3).
                        messages = (
                            f"No VP type mapping for Xero type '{xero_type}'; "
                            f"account not created."
                        )
                        summary['unmapped_type'] += 1
                    elif len(xero_code) > max_account_len:
                        messages = (
                            f"Xero code '{xero_code}' (len {len(xero_code)}) "
                            f"exceeds VP Account max ({max_account_len}); "
                            f"account not created."
                        )
                        summary['skipped_account_code_too_long'] += 1
                    else:
                        resp = VantagepointChartOfAccountsOperator(
                            task_id=f'_post_account_{xero_id}',
                            vp_conn_id=vp_conn_id,
                            request_method='POST',
                            request_body=build_vp_account_create_body(
                                xero_code, xero_name, type_code),
                            pagination=False,
                        ).execute(context)
                        if isinstance(resp, dict):
                            out_code = resp.get('Account', '') or xero_code
                            out_name = resp.get('Name', '') or _truncate_name(xero_name)
                            out_type = resp.get('Type', '') or type_code
                        else:
                            out_code, out_name, out_type = (
                                xero_code, _truncate_name(xero_name), type_code)
                        summary['created'] += 1
                # else: no VP match and not active/creatable → placeholder row.
            elif mapped_vp and vp_code and vp_code == xero_code and vp_name != _truncate_name(xero_name):
                # Existing mapping, VP name drifted from Xero → PUT update.
                update_type = vp_type or type_code or ''
                VantagepointChartOfAccountsOperator(
                    task_id=f'_put_account_{xero_id}',
                    vp_conn_id=vp_conn_id,
                    request_method='PUT',
                    account=vp_code,
                    request_body=build_vp_account_update_body(
                        xero_name, update_type, xero_status),
                    pagination=False,
                ).execute(context)
                out_code, out_name, out_type = (
                    vp_code, _truncate_name(xero_name), update_type)
                summary['updated'] += 1
            else:
                # Already mapped and in sync — keep the resolved VP values.
                out_code = mapped_vp or vp_code
                out_name = vp_name or _truncate_name(xero_name)
                out_type = vp_type

            map_rows.append(_build_map_row(
                xero_code=xero_code, xero_name=xero_name, xero_type=xero_type,
                vp_code=out_code, vp_name=out_name, vp_type=out_type,
                xero_id=str(xero_id), messages=messages,
            ))

        except Exception as exc:  # pylint: disable=broad-exception-caught
            log.error("Failed to sync Xero account %s (%s): %s",
                      xero_id, xero_name, exc)
            summary['errors'].append({
                'xero_id': xero_id, 'name': xero_name, 'error': str(exc),
            })

    # ---- Phase 1b: scoped orphan deactivation (Q6 = A) ----
    # Only VP accounts previously Xero-sourced (in the map) whose XeroID is no
    # longer in the current Xero set are deactivated. Manually-created VP
    # accounts (never in the map) are never touched.
    # Guard: if the Xero fetch returned nothing (failed or empty), skip
    # deactivation entirely — an empty set would falsely orphan every mapped account.
    if not current_xero_ids:
        log.warning(
            "fetch_xero_accounts returned no records — skipping orphan "
            "deactivation to avoid false positives on a failed fetch"
        )
    for existing in (_read_existing_map_rows(context) if current_xero_ids else []):
        xero_id = existing.get('XeroID')
        vp_code = existing.get('VantagepointCode')
        if not xero_id or not vp_code:
            continue
        if str(xero_id) in current_xero_ids:
            continue
        try:
            VantagepointChartOfAccountsOperator(
                task_id=f'_deactivate_orphan_account_{xero_id}',
                vp_conn_id=vp_conn_id,
                request_method='PUT',
                account=vp_code,
                request_body={
                    'Status': 'I',
                    'CashBasisAccount': '',
                    'UnrealizedLossAccount': '',
                    'UnrealizedGainAccount': '',
                    'CashBasisRevaluation': '',
                    'QBOAccountID': '',
                    'Detail': '1',
                },
                pagination=False,
            ).execute(context)
            summary['deactivated_orphans'] += 1
            map_rows.append(_build_map_row(
                xero_code=existing.get('XeroCode') or '',
                xero_name=existing.get('XeroName') or '',
                xero_type=existing.get('XeroType') or '',
                vp_code=vp_code,
                vp_name=existing.get('VantagepointName') or '',
                vp_type=existing.get('VantagepointType') or '',
                xero_id=str(xero_id),
                messages='Deactivated: Xero account no longer present.',
            ))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            log.error("Failed to deactivate orphan VP account %s (XeroID %s): %s",
                      vp_code, xero_id, exc)
            summary['errors'].append({
                'xero_id': xero_id, 'name': existing.get('XeroName'),
                'error': f'orphan deactivation failed: {exc}',
            })

    # ---- Phase 2: single batched upsert keyed XeroID (one S3 lock cycle) ----
    if map_rows:
        S3UpsertCollectionOperator(
            task_id='_upsert_map_chart_of_accounts',
            integration=s3_integration,
            customer=s3_customer,
            integration_type=s3_integration_type,
            collection_name=MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
            key_columns=MAP_CHART_OF_ACCOUNTS_UNIQUE_COLUMNS,
            rows=map_rows,
        ).execute(context)
        log.info("Upserted %d map_chart_of_accounts row(s).", len(map_rows))
    else:
        log.info("No map_chart_of_accounts rows to upsert.")

    log.info("map_chart_of_accounts sync summary: %s", summary)
    if summary['errors']:
        raise RuntimeError(
            f"map_chart_of_accounts sync had {len(summary['errors'])} "
            f"failure(s); first: {summary['errors'][0]}"
        )
    return summary

"""
Account code mapping sync (sections O+P+Q from the pre-split file).

Operator-driven port of the Workato recipe `014_503_psa_sync_account_codes`.
QBO accounts are matched to EXISTING VP Chart-of-Accounts by `AcctNum` OR
`Name` (recipe #17 `va` join) and the matched VP Account/Name/Type is
recorded; a VP account is only CREATED when the QBO account has an `AcctNum`
and no VP match (name-only accounts get a QBO-only row, Workato parity). The
QBO `Classification` → VP type-code lookup uses the static `ACCOUNT_TYPE_MAP`
constant. The DAG stages the QBO/VP/map collections and runs the compile JOIN
via QueryCollectionOperator; this module supplies the collection sources, the
ported SQL, and the step-18 foreach. See fix-log references
(MAP_ACCOUNT_CODE_SYNC_FIX_LOG.md #1-#8).

Public surface (re-exported via `python_callable_method.py`):
    sync_qbo_accounts_to_vp           — step 18 foreach (PythonOperator)
    build_qbo_accounts_staging        — CreateCollectionOperator source (#11)
    prepare_vp_accounts_staging       — CreateCollectionOperator source (#16)
    read_account_code_map_for_staging — CreateCollectionOperator source (#13)
    COMPILE_ACCOUNT_CODES_SQL         — QueryCollectionOperator query (#17)
"""
import rail

# Shared helpers still live in `python_callable_method.py` during the
# staged split.
from vp_quickbooks_integration.mapping_sync.utils._shared import (
    _extract_qbo_records,
    _filter_none,
)
from vp_quickbooks_integration.common.tables import (
    ACCOUNT_TYPE_MAP,
    MAP_ACCOUNT_CODE_TABLE_NAME,
    MAP_ACCOUNT_CODE_UNIQUE_COLUMNS,
)
from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig


# ===========================================================================
# ACCOUNT MAPPING — schema (7 sticky Workato columns)
# ===========================================================================
# QBOCode            = QBO AcctNum or Name fallback
# QBOName
# QBOType            = QBO AccountType
# MAP_ACCOUNT_CODE_TABLE_NAME + MAP_ACCOUNT_CODE_COLUMNS in utils/tables.py.
# The QBOType → VP type lookup is the static `ACCOUNT_TYPE_MAP` Python
# constant in common/tables.py (read-only product config, ported verbatim
# from the Workato account_type_map lookup table — not an S3 collection,
# mirroring PAY_TERMS_MAP / INVOICE_SECTION_CODE_MAP).


# ===========================================================================
# ACCOUNT MAPPING — body builders (QBO Account → VP Chart of Accounts)
# Recipe references:
#   014_503_psa_synch_accounts.recipe.json
#   014_503_psa_sync_account_codes.recipe.json
# ===========================================================================

def _qbo_account_code(qbo_account):
    """VP Account code = QBO AcctNum if present, else QBO Name (Workato fallback)."""
    return qbo_account.get('AcctNum') or qbo_account.get('Name')


def build_vp_account_create_body_from_qbo(qbo_account, vpa_type_code):
    """POST /vision/Accounts/ body. Direction: QBO Account → VP Chart of Accounts.

    Strict Workato parity with `014_503_psa_sync_account_codes.recipe.json`
    POST body lines 2818-2830. Recipe sends:
        Status, Type, Account, Name, CashBasisAccount (blank),
        UnrealizedLossAccount (blank), UnrealizedGainAccount (blank),
        CashBasisRevaluation (blank), QBOAccountID (blank), Detail
    The `=blank` fields are sent as empty strings — VP's schema layer
    enforces column presence and rejects with `Column:<name> does not
    exist` when the key is missing (different error class from "Field
    X does not exist" for unknown fields). Workato's `=blank` evaluates
    to an empty value but the key is still in the JSON body. See
    MAP_ACCOUNT_CODE_SYNC_FIX_LOG.md #2.
    `Description` is NOT a VP /Accounts/ field — sending it returns
    'Field Description does not exist'. See fix #1.
    `Detail: '1'` is hardcoded by the recipe (detail-vs-summary account
    distinction at the VP side).
    `QBOAccountID` is populated with the actual QBO Id (deviation from
    strict Workato parity, which sends blank) — see fix #1 alt 1b.
    """
    qbo_id = qbo_account.get('Id')
    qbo_code = _qbo_account_code(qbo_account)
    qbo_name = qbo_account.get('Name') or ''

    body = {
        'Account': qbo_code,
        'Name': qbo_name,
        'Type': vpa_type_code,
        'Status': 'A',
        'CashBasisAccount': '',
        'UnrealizedLossAccount': '',
        'UnrealizedGainAccount': '',
        'CashBasisRevaluation': '',
        'QBOAccountID': qbo_id,
        'Detail': '1',
    }
    return _filter_none(body)


def build_vp_account_update_body_from_qbo(qbo_account, vpa_type_code):
    """PUT /vision/Accounts/{Account} body. Same fields as the create body
    minus `Account` (the URL segment carries it). Type can shift if QBO
    reclassifies the account, so it stays in the body.

    Workato reference: `014_503_psa_sync_account_codes.recipe.json` PUT
    body lines 3463-3475. See MAP_ACCOUNT_CODE_SYNC_FIX_LOG.md #1, #2.
    """
    qbo_id = qbo_account.get('Id')
    qbo_name = qbo_account.get('Name') or ''

    body = {
        'Name': qbo_name,
        'Type': vpa_type_code,
        'Status': 'A',
        'CashBasisAccount': '',
        'UnrealizedLossAccount': '',
        'UnrealizedGainAccount': '',
        'CashBasisRevaluation': '',
        'QBOAccountID': qbo_id,
        'Detail': '1',
    }
    return _filter_none(body)


# ===========================================================================
# ACCOUNT MAPPING — sync engine (called by map_account_code_dag PythonOperator)
# ===========================================================================

# ===========================================================================
# ACCOUNT MAPPING — collection staging + recipe compile SQL (recipe steps 1-18)
# ===========================================================================
# Operator-driven port of `014_503_psa_sync_account_codes`. The DAG stages
# three run-local collections and runs the recipe's step-17 compile JOIN via
# rail.QueryCollectionOperator (mirroring map_tax_code / abbviemst):
#
#   list VP accounts        -> fetch_vp_accounts (VP GET)            [recipe #3/#6]
#   QBO Accounts            -> collection `qbo_accounts`              [recipe #11]
#   Account Code Map        -> collection `account_code_map`         [recipe #13]
#   Vantagepoint Accounts   -> collection `vp_accounts`              [recipe #16]
#   Compile (3-way JOIN)    -> collection `compiled_account_codes`   [recipe #17]
#   foreach                 -> sync_qbo_accounts_to_vp               [recipe #18]
#
# The account_type_map join (recipe #17 `atm`) is resolved in Python from the
# static ACCOUNT_TYPE_MAP constant (keyed by QBO *Classification*), so it is
# not staged as a collection.

QBO_ACCOUNTS_COLLECTION = 'qbo_accounts'
VP_ACCOUNTS_COLLECTION = 'vp_accounts'
ACCOUNT_CODE_MAP_COLLECTION = 'account_code_map'
COMPILED_ACCOUNT_CODES_COLLECTION = 'compiled_account_codes'

QBO_ACCOUNTS_STAGING_COLUMNS = [
    'AcctNum', 'Name', 'Classification', 'AccountType', 'Id', 'Active',
]
VP_ACCOUNTS_STAGING_COLUMNS = ['Account', 'Name', 'Type', 'QBOAccountID']
ACCOUNT_CODE_MAP_STAGING_COLUMNS = [
    'QBOCode', 'QBOName', 'QBOType', 'VantagepointCode',
    'VantagepointName', 'VantagepointTypeRO', 'QBOID', 'EntryID',
]

# Recipe step 17 — Compile Tax/Account from all sources. Matches QBO accounts
# to existing VP accounts by AcctNum OR Name (the `va` join — several VP
# accounts sharing a name fan out into several rows, e.g. Advertising -> 400
# AND 6000), and to the existing map (`acm`). The account_type_map (`atm`)
# join is resolved in Python from ACCOUNT_TYPE_MAP.
COMPILE_ACCOUNT_CODES_SQL = (
    "SELECT "
    "  qa.AcctNum AS QBOCode, qa.Name AS QBOName, "
    "  qa.Classification AS QBOClassification, qa.AccountType AS QBOType, "
    "  qa.Id AS QBOID, qa.Active AS QBOActive, "
    "  va.Account AS VantagepointCode, va.Name AS VantagepointName, "
    "  va.Type AS VantagepointType, "
    "  acm.VantagepointCode AS MappedVantagepointCode, acm.EntryID AS EntryID "
    "FROM qbo_accounts qa "
    "LEFT JOIN vp_accounts va "
    "  ON qa.AcctNum = va.Account OR qa.Name = va.Name "
    "LEFT JOIN account_code_map acm ON qa.Id = acm.QBOID "
    "ORDER BY qa.Name"
)

# VP Chart-of-Accounts `Account` column limit (DB-side truncation guard).
# Workato reads the tenant's actual limit from VP System Formats
# (`formats.first.AccountLength`, recipe #29); `_resolve_vp_account_max_len`
# does the same from the DAG's `get_system_formats` task, falling back to
# this observed default when the format can't be determined.
_VP_ACCOUNT_CODE_DEFAULT_MAX_LEN = 13

# VP System Formats doesn't label the account-number length field
# consistently across versions; probe the common spellings (mirrors
# chart_of_accounts_sync._MAX_LENGTH_CANDIDATE_KEYS).
_MAX_ACCOUNT_LENGTH_CANDIDATE_KEYS = (
    'AccountLength', 'AcctLength', 'MaxLength', 'Length', 'Size', 'FieldLength',
)


def _resolve_vp_account_max_len(context):  # pylint: disable=unused-argument
    """VP Account-code max length from the `get_system_formats` task result
    (recipe #29 `formats.first.AccountLength`), else the observed default.

    Degrades gracefully: any unexpected response shape (or a missing task)
    falls back to `_VP_ACCOUNT_CODE_DEFAULT_MAX_LEN` rather than blocking
    AcctNum-based creates.
    """
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


def build_qbo_accounts_staging(**_context):
    """CreateCollectionOperator source for `qbo_accounts` (recipe #11)."""
    return [
        {
            'AcctNum': a.get('AcctNum') or '',
            'Name': a.get('Name') or '',
            'Classification': a.get('Classification') or '',
            'AccountType': a.get('AccountType') or '',
            'Id': str(a.get('Id') or ''),
            'Active': a.get('Active', True),
        }
        for a in _extract_qbo_records(rail.result('fetch_qbo_accounts'))
        if a.get('Id')
    ]


def prepare_vp_accounts_staging(**_context):
    """CreateCollectionOperator source for `vp_accounts` (recipe #16)."""
    result = rail.result('fetch_vp_accounts')
    if isinstance(result, dict):
        records = [result]
    elif isinstance(result, list):
        records = result
    else:
        records = []
    return [
        {
            'Account': r.get('Account') or '',
            'Name': r.get('Name') or '',
            'Type': r.get('Type') or '',
            'QBOAccountID': r.get('QBOAccountID') or '',
        }
        for r in records
        if isinstance(r, dict) and r.get('Account')
    ]


def read_account_code_map_for_staging(**_context):
    """CreateCollectionOperator source for `account_code_map` (recipe #13).

    Copies the persistent S3 map_account_code rows (plus the sqlite rowid as
    EntryID) into the run-local collection so the step-17 JOIN can read the
    existing mapping (`acm`). Empty on a first sync.
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
                f'SELECT rowid, QBOCode, QBOName, QBOType, VantagepointCode, '
                f'VantagepointName, VantagepointTypeRO, QBOID '
                f'FROM {MAP_ACCOUNT_CODE_TABLE_NAME}'
            )
            for (entry_id, qbo_code, qbo_name, qbo_type, vp_code, vp_name,
                 vp_type_ro, qbo_id) in cur.fetchall():
                rows.append({
                    'QBOCode': qbo_code, 'QBOName': qbo_name, 'QBOType': qbo_type,
                    'VantagepointCode': vp_code, 'VantagepointName': vp_name,
                    'VantagepointTypeRO': vp_type_ro, 'QBOID': qbo_id,
                    'EntryID': entry_id,
                })
    return rows


def _read_compiled_account_codes(context):
    """Read the run-local `compiled_account_codes` collection (recipe #17 output)."""
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


def _resolve_account_type_code(classification):
    """QBO Classification -> VP type code via ACCOUNT_TYPE_MAP (recipe #17
    `IFNULL(atm.VantagepointCode, 1)`). Falls back to '1' when the
    Classification isn't one of the 5 mapped values."""
    entry = ACCOUNT_TYPE_MAP.get(classification or '')
    return entry['code'] if entry else '1'


def _build_map_account_code_row(qbo_code, qbo_name, qbo_type, vp_code,
                                vp_name, vp_type_ro, qbo_id):
    """Assemble one map_account_code row dict for the batched upsert.

    Keys cover every column in MAP_ACCOUNT_CODE_COLUMNS (the upsert
    operator builds its ON CONFLICT statement from the first row's keys,
    so all rows must share this exact column set). The natural key is
    (QBOID, VantagepointCode) — see MAP_ACCOUNT_CODE_UNIQUE_COLUMNS — which
    lets a QBO account fan out to several VP codes (Advertising → 400 AND
    6000) while collapsing QBO-only rows to a single (QBOID, '') row.
    """
    return {
        'QBOCode': qbo_code,        # QBO AcctNum (recipe #20 col1; empty when none)
        'QBOName': qbo_name,
        'QBOType': qbo_type,        # col3 = QBO AccountType (matches target CSV)
        'VantagepointCode': vp_code,
        'VantagepointName': vp_name,
        'VantagepointTypeRO': vp_type_ro,
        'QBOID': str(qbo_id),
    }


def sync_qbo_accounts_to_vp(instance):  # pylint: disable=unused-argument,too-many-locals,too-many-branches,too-many-statements
    """Forward sync (QBO Account → VP Chart of Accounts) — recipe step 18.

    Recipe steps 1-17 are performed upstream by the DAG's collection
    operators, which materialize `compiled_account_codes`. For each
    compiled row (recipe foreach #18):

      - #22/#23/#24 — matched an existing VP account by AcctNum/Name and the
        names are equal → record that VP account's Account/Name/Type into the
        map (no create). Several VP accounts sharing a name produce several
        map rows (e.g. Advertising → 400 AND 6000).
      - #26/#27 — no VP match AND the QBO account has an AcctNum → create the
        VP account (Account = AcctNum). Accounts with NO AcctNum are NOT
        created; they get a QBO-only map row with empty VP code (Workato
        parity — this is why the trial tenant's name-only accounts stay
        unmapped instead of POSTing the Name as the code).
      - #37 — already mapped and only the name drifted → PUT the VP name.

    Two-phase shape (lock-window minimization):
      1. Do ALL VP API work (POST creates / PUT updates) first, accumulating
         the resulting map rows in memory. No S3 collection is opened during
         the HTTP calls.
      2. Hand the accumulated rows to ONE S3UpsertCollectionOperator call —
         a single download/modify/upload/lock cycle for the whole run. The
         old shape held the collection open and locked across every VP HTTP
         call; this confines the lock to the batched write.

    The upsert is keyed on (QBOID, VantagepointCode) — see
    MAP_ACCOUNT_CODE_UNIQUE_COLUMNS, declared as a UNIQUE index by
    dispatcher_dag.init_mapping_collections — so re-runs converge (non-key
    columns are refreshed from the proposed row via ON CONFLICT DO UPDATE).

    Reads:
      - run-local `compiled_account_codes` (DAG query_compiled_account_codes)
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
    log.info("Read %d compiled account rows (recipe #17 output)",
             len(compiled_rows))

    # VP Account-code max length from System Formats (recipe #29), resolved
    # once for the whole run. Guards AcctNum-based creates against VP's
    # CA.Account column width.
    max_account_len = _resolve_vp_account_max_len(context)
    log.info("VP max account-code length: %d", max_account_len)

    summary = {
        'created': 0, 'updated': 0, 'matched_existing': 0,
        'qbo_only_no_vp': 0, 'skipped_account_code_too_long': 0,
        'errors': [],
    }

    # ---- Phase 1: all VP API work, accumulate map rows in memory ----
    # Nothing touches S3 here, so the collection lock is NOT held across the
    # VP POST/PUT round-trips.
    map_rows = []

    for row in compiled_rows:
        qbo_id = row.get('QBOID')
        if not qbo_id:
            continue
        qbo_name = row.get('QBOName') or ''
        qbo_code = row.get('QBOCode') or ''            # AcctNum
        qbo_classification = row.get('QBOClassification') or ''
        qbo_type = row.get('QBOType') or ''            # AccountType (stored col3)
        vp_code = row.get('VantagepointCode') or ''    # va.Account
        vp_name = row.get('VantagepointName') or ''    # va.Name
        vp_type = row.get('VantagepointType') or ''    # va.Type
        mapped_vp = row.get('MappedVantagepointCode') or ''

        # VP columns to record for THIS compiled row (one map row).
        out_code, out_name, out_type = '', '', ''
        do_write = True

        try:
            if not mapped_vp:
                # #23/#24: matched an existing VP account by name → record it.
                if vp_code and qbo_name == vp_name:
                    out_code, out_name, out_type = vp_code, vp_name, vp_type
                    summary['matched_existing'] += 1
                # #26/#27: no VP match → create only when AcctNum present.
                elif not vp_code and qbo_code:
                    if len(qbo_code) > max_account_len:
                        log.warning(
                            "QBO account %s (%s): AcctNum %r length %d "
                            "exceeds VP Account max (%d); storing QBO-only row.",
                            qbo_id, qbo_name, qbo_code, len(qbo_code),
                            max_account_len,
                        )
                        summary['skipped_account_code_too_long'] += 1
                    else:
                        type_code = _resolve_account_type_code(qbo_classification)
                        create_body = build_vp_account_create_body_from_qbo(
                            {'Id': qbo_id, 'AcctNum': qbo_code, 'Name': qbo_name},
                            type_code,
                        )
                        try:
                            resp = VantagepointChartOfAccountsOperator(
                                task_id=f'_post_account_{qbo_id}',
                                vp_conn_id=vp_conn_id,
                                request_method='POST',
                                request_body=create_body,
                                pagination=False,
                            ).execute(context)
                        except Exception as create_exc:  # pylint: disable=broad-exception-caught
                            # Defensive: VP already has this Account code.
                            if 'already exists' not in str(create_exc).lower():
                                raise
                            out_code, out_name, out_type = qbo_code, qbo_name, type_code
                            summary['matched_existing'] += 1
                            log.warning(
                                "QBO account %s (%s): VP POST 'already "
                                "exists'; adopting code %r.",
                                qbo_id, qbo_name, qbo_code,
                            )
                        else:
                            if isinstance(resp, dict):
                                out_code = resp.get('Account', '') or qbo_code
                                out_name = resp.get('Name', '') or qbo_name
                                out_type = resp.get('Type', '') or type_code
                            else:
                                out_code, out_name, out_type = qbo_code, qbo_name, type_code
                            summary['created'] += 1
                else:
                    # No VP match and no AcctNum → QBO-only row (Workato parity).
                    summary['qbo_only_no_vp'] += 1
            elif mapped_vp and vp_code and vp_code == qbo_code and vp_name != qbo_name:
                # #37: existing mapping, VP name drifted from QBO → PUT update.
                type_code = _resolve_account_type_code(qbo_classification)
                update_body = build_vp_account_update_body_from_qbo(
                    {'Id': qbo_id, 'Name': qbo_name}, type_code,
                )
                VantagepointChartOfAccountsOperator(
                    task_id=f'_put_account_{qbo_id}',
                    vp_conn_id=vp_conn_id,
                    request_method='PUT',
                    account=vp_code,
                    request_body=update_body,
                    pagination=False,
                ).execute(context)
                out_code, out_name, out_type = vp_code, qbo_name, vp_type
                summary['updated'] += 1
            else:
                # Already mapped, nothing to change — leave the row as-is.
                do_write = False

            if do_write:
                map_rows.append(_build_map_account_code_row(
                    qbo_code, qbo_name, qbo_type,
                    out_code, out_name, out_type, qbo_id,
                ))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            log.error("Failed to sync QBO account %s (%s): %s",
                      qbo_id, qbo_name, exc)
            summary['errors'].append({
                'qbo_id': qbo_id, 'name': qbo_name, 'error': str(exc),
            })

    # ---- Phase 2: single batched upsert (one S3 lock cycle) ----
    # All accumulated rows go up in ONE download/modify/upload/lock cycle via
    # the canonical S3 collection operator, keyed on (QBOID, VantagepointCode).
    if map_rows:
        S3UpsertCollectionOperator(
            task_id='_upsert_map_account_code',
            integration=IntegrationConfig.S3_INTEGRATION_NAME,
            customer=IntegrationConfig.get_s3_customer(context),
            integration_type=IntegrationConfig.get_s3_integration_type(context),
            collection_name=MAP_ACCOUNT_CODE_TABLE_NAME,
            key_columns=MAP_ACCOUNT_CODE_UNIQUE_COLUMNS,
            rows=map_rows,
        ).execute(context)
        log.info("Upserted %d map_account_code row(s) in one S3 cycle.",
                 len(map_rows))
    else:
        log.info("No map_account_code rows to upsert.")

    log.info("map_account_code sync summary: %s", summary)
    if summary['errors']:
        raise RuntimeError(
            f"map_account_code sync had {len(summary['errors'])} failure(s); "
            f"first: {summary['errors'][0]}"
        )
    return summary


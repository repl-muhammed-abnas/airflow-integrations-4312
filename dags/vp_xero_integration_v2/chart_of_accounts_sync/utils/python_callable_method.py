"""
Python callable methods for Xero -> VP Chart of Accounts Sync.

Ports the Workato `chart_of_accounts` bundle (recipes
`014_501_psa_poll_xero_chart_of_accounts` + `014_501_psa_sync_accounts`, plus
the two lookup tables) into Python callables for the 3-DAG Airflow template
(main -> dispatcher -> processor).

Workato lookup tables:
  - Chart-of-Accounts Map (sync-state crosswalk, keyed by the Xero AccountID) ->
    shared mapping_sync `map_chart_of_accounts` S3 collection.
  - Account-Type Map (Xero Type enum -> VP numeric type) -> the in-code
    `common.tables.ACCOUNT_TYPE_SEED_ROWS` (16 rows). Product-level constant, not
    tenant-specific, so no S3 dependency and no reliance on mapping_sync's init
    having seeded the collection.

The Workato worker runs a SQL JOIN (Xero LEFT JOIN VP on Code=Account, LEFT JOIN
the code-map on AccountID=XeroID OR Account=VantagepointCode, LEFT JOIN the
type-map on UPPER(Type)) and a per-account FOREACH with these outcomes:
add-to-map, link-in-map, create VP account, backfill the map's Xero side,
update VP account, skip. BANK accounts are excluded throughout.

Here the dispatcher fetches the changed Xero accounts (via If-Modified-Since) +
the full VP account list (slim index in conf); the processor reproduces the
FOREACH decision tree for one Xero account at a time.

Re-run safety is watermark-only — the dispatcher advances the watermark only on
a fully clean run, and the map's XeroID key keeps already-linked accounts from
being re-created.
"""
# pylint: disable=invalid-name,broad-exception-caught,too-many-return-statements
import logging
from airflow.models import Variable
import rail
# Shared collection helpers + table/column constants come from common so the S3
# access logic and SQLite identifiers can't drift across integrations.
from vp_xero_integration_v2.common.python_callable_method import (
    collection_rows,
    collection_update,
    collection_upsert,
    unwrap_vp_response,
)
from vp_xero_integration_v2.mapping_sync.utils._shared import _extract_xero_records
from vp_xero_integration_v2.common.tables import (
    MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
    MAP_CHART_OF_ACCOUNTS_COLUMNS,
    MAP_CHART_OF_ACCOUNTS_UNIQUE_COLUMNS,
    ACCOUNT_TYPE_SEED_ROWS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config Variables (optional overrides; defaults keep parse-time DAGs valid).
# ---------------------------------------------------------------------------
# Optional global override for VP's maximum account-number length (Workato reads
# it from VP System Formats; we fetch that too, but an explicit override wins).
_MAX_ACCOUNT_LENGTH_VARIABLE_KEY = (
    'vp_xero_chart_of_accounts_sync_max_account_length'
)
# VP System Formats entity filter for the account-number format. Configurable
# because the exact entity token is environment/version dependent.
_SYSTEM_FORMATS_ENTITY_VARIABLE_KEY = (
    'vp_xero_chart_of_accounts_sync_system_formats_entity'
)
_DEFAULT_SYSTEM_FORMATS_ENTITY = '?entity=account'

# Workato type-map `IFNULL(..., 1)` — unmapped Xero types default to VP type 1
# (Asset).
_DEFAULT_VP_TYPE = '1'

# VP truncates account Name to 39 chars (Workato `XeroName.slice(0,39)`).
_VP_ACCOUNT_NAME_MAX_LEN = 39

# VP Account columns the Workato create/update payload always sends blank
# (`=blank`). VP's /Accounts/ handler references these columns and rejects the
# request when they are absent, so we send them as empty strings on POST + PUT.
_BLANK_VP_ACCOUNT_FIELDS = (
    'CashBasisAccount',
    'UnrealizedLossAccount',
    'UnrealizedGainAccount',
    'CashBasisRevaluation',
    'QBOAccountID',
)

# Candidate keys probed in a VP System Formats record to discover the max
# account-number length (the payload field name is not contractual).
_MAX_LENGTH_CANDIDATE_KEYS = (
    'AccountLength', 'AcctLength', 'MaxLength', 'Length', 'Size', 'FieldLength',
)

# Xero Account status considered "active"; anything else (ARCHIVED, DELETED)
# propagates to VP as Inactive on update (Workato `Status == "ACTIVE" ? A : I`).
_XERO_STATUS_ACTIVE = 'ACTIVE'

# Xero account Type enum -> VP numeric type code, built once from the seeded
# rows and keyed on the uppercased Xero Type (Workato joins on UPPER(Type)).
_XERO_TYPE_TO_VP_TYPE = {
    str(xero_type).strip().upper(): str(vp_code)
    for (_desc, xero_type, vp_code) in ACCOUNT_TYPE_SEED_ROWS
}


# ---------------------------------------------------------------------------
# Generic helpers (shared dispatcher + processor)
# ---------------------------------------------------------------------------
def _conf():
    """The current DAG run's conf dict."""
    return rail.get_current_context()['dag_run'].conf or {}


def _conf_value(key, default=''):
    """Fetch a single key out of the current dag_run.conf."""
    value = _conf().get(key)
    return value if value is not None else default


def _s(value):
    """Coerce a field to a stripped string.

    VP and Xero operators return numeric fields as ints/floats on the wire (e.g.
    the VP account `Type` comes back as `9`, not `"9"`), so a bare `.strip()`
    blows up with AttributeError. Coerce defensively; None -> ''.
    """
    if value is None:
        return ''
    return str(value).strip()


def _truncate_name(name):
    """VP truncates account Name to 39 chars (Workato `XeroName.slice(0,39)`)."""
    return _s(name)[:_VP_ACCOUNT_NAME_MAX_LEN]


def _xero_fields():
    """Extract the Xero account identity from the processor's conf.

    The dispatcher spreads the raw Xero Account record into conf. Xero field
    names: AccountID, Code, Name, Status, Type.
    """
    conf = _conf()
    return {
        'xero_id': _s(conf.get('AccountID')),
        'xero_code': _s(conf.get('Code')),
        'xero_name': _s(conf.get('Name')),
        'xero_status': _s(conf.get('Status')).upper(),
        'xero_type': _s(conf.get('Type')),
    }



def map_xero_type_to_vp(xero_type):
    """Map a Xero account Type to a VP numeric type code (case-insensitive).

    Mirrors the Workato Account-Type Map join on UPPER(Type) with the
    `IFNULL(..., 1)` default, sourced from the in-code
    `common.tables.ACCOUNT_TYPE_SEED_ROWS` (16 rows). Unmapped types fall back
    to _DEFAULT_VP_TYPE ('1' Asset).
    """
    key = _s(xero_type).upper()
    if not key:
        return _DEFAULT_VP_TYPE
    return _XERO_TYPE_TO_VP_TYPE.get(key, _DEFAULT_VP_TYPE)


def _account_insert(values, context=None):
    """INSERT one map_chart_of_accounts row. Column order from common.tables so
    the SQLite identifiers can't drift; missing keys default to ''."""
    context = context or rail.get_current_context()
    columns = ', '.join(MAP_CHART_OF_ACCOUNTS_COLUMNS)
    placeholders = ', '.join(['?'] * len(MAP_CHART_OF_ACCOUNTS_COLUMNS))
    collection_update(
        MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
        f"INSERT OR IGNORE INTO {MAP_CHART_OF_ACCOUNTS_TABLE_NAME} ({columns}) "
        f"VALUES ({placeholders})",
        [values.get(col, '') for col in MAP_CHART_OF_ACCOUNTS_COLUMNS],
        context,
    )


def _write_map_account_row(values, context=None):
    """Upsert a map_chart_of_accounts row keyed by XeroID via a single atomic
    S3UpsertCollectionOperator call (defensive fallback when an in-place rowid
    update isn't available). Matches MAP_CHART_OF_ACCOUNTS_UNIQUE_COLUMNS."""
    context = context or rail.get_current_context()
    collection_upsert(
        MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
        MAP_CHART_OF_ACCOUNTS_UNIQUE_COLUMNS,
        {col: values.get(col, '') for col in MAP_CHART_OF_ACCOUNTS_COLUMNS},
        context,
    )


# ---------------------------------------------------------------------------
# Dispatcher callables
# ---------------------------------------------------------------------------
def extract_account_list_method():
    """Extract the changed Xero account list, excluding BANK accounts.

    Workato parity: the poll trigger fired only for `Type != "BANK"`, and the
    worker's compile JOIN also filtered `WHERE Type != 'BANK'`. We drop BANK
    accounts here so no processor DAG is ever spawned for one.
    """
    records = _extract_xero_records(rail.result('get_recently_changed_accounts'))
    accounts = [
        acct for acct in records
        if isinstance(acct, dict) and _s(acct.get('Type')).upper() != 'BANK'
    ]
    logger.info(
        "Found %d changed Xero accounts (%d after excluding BANK)",
        len(records), len(accounts)
    )
    return accounts


def build_vp_account_index_method():
    """Slim the full VP /Accounts list down to {Account, Name, Type} rows.

    This index rides in each processor's conf so the processor can reproduce the
    Workato `Code = Account` match without each child re-listing every VP
    account.
    """
    rows = unwrap_vp_response(rail.result('get_all_vp_accounts'), strict=True)
    index = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        index.append({
            'Account': _s(row.get('Account')),
            'Name': _s(row.get('Name')),
            'Type': _s(row.get('Type')),
        })
    logger.info("Built slim VP account index with %d entries", len(index))
    return index


def check_if_accounts_exist_method():
    """IfOperator test: did the Xero poll surface any (non-BANK) changed accounts?"""
    return len(rail.result('extract_account_list') or []) > 0


def build_processor_dag_conf(item):
    """Build the per-account conf for the processor DAG.

    `item` is one Xero Account record (AccountID/Code/Name/Status/Type). We
    spread it in and attach the slim VP index plus the connections/customerId/
    integrationType carried from the main DAG (customerId locates the shared
    mapping_sync S3 collection).

    NOTE: connections and customerId are injected by the dispatcher's
    create_dag wrapper (V2 architecture); this base function leaves them
    unset so the wrapper can override cleanly.
    """
    ctx_conf = rail.get_current_context()['dag_run'].conf or {}
    return {
        **item,
        'vp_account_index': rail.result('build_vp_account_index'),
        'connections': ctx_conf.get('connections'),
        'customerId': ctx_conf.get('customerId'),
        'integrationType': ctx_conf.get('integrationType'),
    }


# ---------------------------------------------------------------------------
# Processor callables — the per-account decision tree (Workato FOREACH body)
# ---------------------------------------------------------------------------
def ensure_map_row_method():
    """Workato step 16-18: ensure this Xero account has a crosswalk row.

    Match order mirrors the Workato JOIN (`AccountID = XeroID OR Account =
    VantagepointCode`):
      1. by XeroID (the account's own map row);
      2. by VantagepointCode == this Xero Code with a blank XeroID (the
         branch-3 backfill case — a map row created from the VP side that never
         recorded the Xero identity).
    Only INSERT a fresh blank row when the account is ACTIVE (Workato step 16
    gates on `XeroStatus == ACTIVE`). Idempotent: an existing row is returned
    untouched so we never clobber an established VP link. Returns the row (with
    its sqlite `_rowid`) or {} when no row exists and none was created.
    """
    fields = _xero_fields()
    xero_id = fields['xero_id']
    if not xero_id:
        raise RuntimeError(
            "Processor dag_run.conf missing Xero AccountID — cannot key the "
            "map_chart_of_accounts. Refusing to process an account with no "
            "identity."
        )
    context = rail.get_current_context()

    # 1. by XeroID.
    rows = collection_rows(
        MAP_CHART_OF_ACCOUNTS_TABLE_NAME, MAP_CHART_OF_ACCOUNTS_COLUMNS,
        "XeroID = ?", [xero_id], context,
    )
    if rows:
        logger.info(
            "Xero account %s already in map_chart_of_accounts — leaving as is",
            xero_id
        )
        return rows[0]

    # 2. by VantagepointCode == Code with a blank XeroID (backfill candidate).
    xero_code = fields['xero_code']
    if xero_code:
        orphan = collection_rows(
            MAP_CHART_OF_ACCOUNTS_TABLE_NAME, MAP_CHART_OF_ACCOUNTS_COLUMNS,
            "VantagepointCode = ? AND (XeroID IS NULL OR XeroID = '')",
            [xero_code], context,
        )
        if orphan:
            logger.info(
                "Found orphan map row for VP code %s (blank XeroID) — "
                "candidate for Xero-side backfill", xero_code
            )
            return orphan[0]

    # 3. No row anywhere. Only add one for ACTIVE accounts (Workato step 16).
    if fields['xero_status'] != _XERO_STATUS_ACTIVE:
        logger.info(
            "Xero account %s (%s) is not ACTIVE and has no map row — nothing "
            "to add", xero_id, xero_code
        )
        return {}

    blank_row = {
        'XeroCode': xero_code,
        'XeroName': fields['xero_name'],
        'XeroType': fields['xero_type'],
        'VantagepointCode': '',
        'VantagepointName': '',
        'VantagepointType': '',
        'XeroID': xero_id,
        'Messages': '',
    }
    _account_insert(blank_row, context)
    # Re-read so downstream tasks get the row WITH its sqlite _rowid (used by
    # link_account_in_map for an in-place update — Workato's update-by-EntryID).
    rows = collection_rows(
        MAP_CHART_OF_ACCOUNTS_TABLE_NAME, MAP_CHART_OF_ACCOUNTS_COLUMNS,
        "XeroID = ?", [xero_id], context,
    )
    logger.info(
        "Added Xero account %s (%s) to map_chart_of_accounts", xero_id,
        xero_code
    )
    return rows[0] if rows else blank_row


def match_vp_account_method():
    """Workato JOIN: match this Xero account to a VP account BY CODE.

    The Xero Workato recipe (`014_501_psa_sync_accounts`) joins Xero to VP on
    `xa.Code = va.Account` only — there is NO name-based join (unlike the QBO
    recipe, which joins on `AcctNum OR Name`). The VP account `Name` is used
    solely as an equality GUARD on the code-matched row (see `decide_action`'s
    link/backfill branches), never as a match key. So we return just the
    code-matched VP row:
        {'code_match': <VP row where Account == Xero Code, or None>}
    """
    fields = _xero_fields()
    xero_code = fields['xero_code']
    index = _conf_value('vp_account_index', []) or []

    code_match = None
    for vp in index:
        if not isinstance(vp, dict):
            continue
        if xero_code and _s(vp.get('Account')) == xero_code:
            code_match = vp
            break
    logger.info(
        "VP match for Xero account %s (code=%r): code_match=%s",
        fields['xero_id'], xero_code, bool(code_match)
    )
    return {'code_match': code_match}


def decide_action_method():
    """Reproduce the Workato per-account decision tree.

    Returns a decision dict:
        {'action', 'vp_code', 'vp_name', 'vp_type',
         'xero_code', 'xero_name', 'xero_type', 'xero_id'}
    where action is one of 'create' | 'update' | 'link' | 'backfill' | 'skip'.
    The vp_*/xero_* fields carry the reconciled values that
    `link_account_in_map_method` persists into the crosswalk row after the
    (optional) VP API call.

    The three map states are mutually exclusive on (MappedVantagepointCode,
    MappedXeroID):
      - MappedVantagepointCode blank            -> Workato step 19 (link/create)
      - MappedVantagepointCode set, XeroID blank -> Workato step 33 (backfill)
      - MappedVantagepointCode set, XeroID set   -> Workato step 35 (update)
    """
    fields = _xero_fields()
    xero_id = fields['xero_id']
    xero_code = fields['xero_code']
    xero_name = fields['xero_name']

    row = rail.result('ensure_map_row') or {}
    mapped_vp_code = _s(row.get('VantagepointCode'))
    mapped_xero_id = _s(row.get('XeroID'))

    match = rail.result('match_vp_account') or {}
    code_match = match.get('code_match')

    decision = {
        'action': 'skip',
        'vp_code': '', 'vp_name': '', 'vp_type': '',
        'xero_code': xero_code, 'xero_name': xero_name,
        'xero_type': fields['xero_type'], 'xero_id': xero_id,
    }

    # --- Workato step 19: not yet linked to a VP account in the map. ---
    if not mapped_vp_code:
        if code_match:
            # A VP account with the same code already exists (VantagepointCode
            # present). Workato step 21: link ONLY when code + name both match;
            # a name mismatch is left for a human -> skip.
            vp_account = _s(code_match.get('Account'))
            vp_name = _s(code_match.get('Name'))
            if vp_account == xero_code and vp_name == _truncate_name(xero_name):
                decision.update(
                    action='link',
                    vp_code=vp_account,
                    vp_name=vp_name,
                    vp_type=_s(code_match.get('Type')),
                )
        elif fields['xero_status'] == _XERO_STATUS_ACTIVE and xero_code:
            # Workato step 23: no VP account exists + ACTIVE -> create.
            decision.update(
                action='create',
                vp_code=xero_code,
                vp_name=xero_name,
                vp_type=map_xero_type_to_vp(fields['xero_type']),
            )
        logger.info(
            "decide_action(Xero %s): unlinked -> %s", xero_id,
            decision['action']
        )
        return decision

    # --- Workato step 33: map has a VP code but no Xero identity yet. ---
    if not mapped_xero_id:
        # Backfill the Xero side ONLY when a live VP account matches and its
        # name equals the Xero name (Workato step 33 guard). Map-only, no VP
        # write. Preserve the existing VP columns from the map row.
        if code_match and _s(code_match.get('Name')) == _truncate_name(xero_name):
            decision.update(
                action='backfill',
                vp_code=mapped_vp_code,
                vp_name=_s(row.get('VantagepointName')),
                vp_type=_s(row.get('VantagepointType')),
            )
        logger.info(
            "decide_action(Xero %s): mapped VP code, blank XeroID -> %s",
            xero_id, decision['action']
        )
        return decision

    # --- Workato step 35: fully linked pair -> update the VP account. ---
    # Requires the live VP account to still exist (VantagepointCode present in
    # the join). Name (<=39) + Status (A/I) + Type are pushed to VP; Type keeps
    # the EXISTING VP type (Workato PUT sends `Type = VantagepointType`, i.e.
    # va.Type — it does NOT remap here, unlike create).
    if code_match:
        decision.update(
            action='update',
            vp_code=mapped_vp_code,
            vp_name=xero_name,
            vp_type=_s(code_match.get('Type')) or _s(row.get('VantagepointType')),
        )
    logger.info(
        "decide_action(Xero %s): fully linked -> %s", xero_id,
        decision['action']
    )
    return decision


def _action_is(action):
    """IfOperator test factory: is decide_action's chosen action == `action`?"""
    return (rail.result('decide_action') or {}).get('action') == action


def is_create_action():
    """IfOperator test: create a new VP account?"""
    return _action_is('create')


def is_update_action():
    """IfOperator test: update an existing VP account?"""
    return _action_is('update')


def is_link_action():
    """IfOperator test: link an existing VP account into the map (no VP write)?"""
    return _action_is('link')


def is_backfill_action():
    """IfOperator test: backfill the map row's Xero side (no VP write)?"""
    return _action_is('backfill')


# ---------------------------------------------------------------------------
# Create pre-flight: account-number length guard (Workato System Formats step)
# ---------------------------------------------------------------------------
def system_formats_entity_filter():
    """Filter string for the VP System Formats fetch (account-number format)."""
    return Variable.get(
        _SYSTEM_FORMATS_ENTITY_VARIABLE_KEY,
        default_var=_DEFAULT_SYSTEM_FORMATS_ENTITY,
    )


def _extract_max_account_length():
    """Resolve VP's max account-number length, or None if undeterminable.

    Precedence: explicit override Variable > value probed from the System
    Formats response. Returns an int, or None when neither yields a number (the
    caller then skips the guard rather than blocking creates).
    """
    override = Variable.get(_MAX_ACCOUNT_LENGTH_VARIABLE_KEY, default_var='')
    if override and str(override).strip().isdigit():
        return int(str(override).strip())

    rows = unwrap_vp_response(rail.result('get_system_formats'), strict=False)
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in _MAX_LENGTH_CANDIDATE_KEYS:
            value = row.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text.isdigit() and int(text) > 0:
                return int(text)
    return None


def check_account_code_length_method():
    """Workato pre-flight: refuse to create when the Xero code is too long.

    Raises (routing to error capture) with the Workato message when the Xero
    account number exceeds VP's max length. When the max length can't be
    determined, logs a warning and proceeds — degrading gracefully rather than
    blocking every create on an unknown System Formats shape.
    """
    fields = _xero_fields()
    xero_code = fields['xero_code']
    xero_name = fields['xero_name']
    max_len = _extract_max_account_length()
    if max_len is None:
        logger.warning(
            "Could not determine VP max account length from System Formats or "
            "the '%s' override Variable — skipping the account-number length "
            "guard for this create.", _MAX_ACCOUNT_LENGTH_VARIABLE_KEY
        )
        return True
    if len(xero_code) > max_len:
        raise RuntimeError(
            "Failed to add GLAccount to Vantagepoint from Xero. "
            f"(account: {xero_name}, number: {xero_code}) Account number "
            f"exceeds maximum permitted number of characters as set in "
            f"Vantagepoint ({max_len})."
        )
    logger.info(
        "Account number %r within VP max length %s — OK", xero_code, max_len
    )
    return True


# ---------------------------------------------------------------------------
# VP request-body builders
# ---------------------------------------------------------------------------
def build_create_account_body_method():
    """POST /Accounts/ body for a new VP GL account (Workato step 29).

    Account = Xero Code, Name truncated to 39, Status = A, Type = mapped VP
    type, Detail = 1; the balancing-account + QBOAccountID columns sent blank.
    """
    decision = rail.result('decide_action') or {}
    fields = _xero_fields()
    body = {
        'Account': decision.get('vp_code') or fields['xero_code'],
        'Name': _truncate_name(fields['xero_name']),
        'Status': 'A',
        'Type': decision.get('vp_type') or _DEFAULT_VP_TYPE,
        'Detail': '1',
    }
    for field in _BLANK_VP_ACCOUNT_FIELDS:
        body[field] = ''
    logger.info("Built VP create body for account %s: %s", body['Account'], body)
    return body


def build_update_account_body_method():
    """PUT /Accounts/{code} body (Workato step 37).

    Account = mapped VP code, Name truncated to 39, Type = existing VP type,
    Status = 'A' when the Xero account is ACTIVE else 'I' (deactivation
    propagates); balancing-account + QBOAccountID columns blank.
    """
    decision = rail.result('decide_action') or {}
    fields = _xero_fields()
    status = 'A' if fields['xero_status'] == _XERO_STATUS_ACTIVE else 'I'
    body = {
        'Account': decision.get('vp_code') or fields['xero_code'],
        'Name': _truncate_name(fields['xero_name']),
        'Status': status,
        'Type': decision.get('vp_type') or _DEFAULT_VP_TYPE,
        'Detail': '1',
    }
    for field in _BLANK_VP_ACCOUNT_FIELDS:
        body[field] = ''
    logger.info("Built VP update body for account %s: %s", body['Account'], body)
    return body


# ---------------------------------------------------------------------------
# Map linking (Workato writes back into the crosswalk row after each action)
# ---------------------------------------------------------------------------
def link_account_in_map_method():
    """Persist the reconciled crosswalk row after link / create / update / backfill.

    Writes the full set of Xero + VP columns IN PLACE by the sqlite `_rowid`
    (Workato's update_entry-by-EntryID), leaving the row identity intact. This
    is idempotent and covers every non-skip outcome:
      - link:     VP code/name/type from the matched VP account.
      - create:   VP code/name/type from the newly created VP account
                  (decide_action carried the Xero code as the new VP code).
      - update:   VP name synced to the (truncated) Xero name; code/type kept.
      - backfill: Xero code/name/type/id written onto a VP-side-only map row.
    """
    fields = _xero_fields()
    decision = rail.result('decide_action') or {}
    row = rail.result('ensure_map_row') or {}

    action = decision.get('action')
    # VP name written to the map mirrors what we sent to VP (truncated) for
    # create/update; for link/backfill it's the matched/existing VP name.
    if action in ('create', 'update'):
        vp_name = _truncate_name(fields['xero_name'])
    else:
        vp_name = decision.get('vp_name') or _s(row.get('VantagepointName'))

    values = {
        'XeroCode': decision.get('xero_code') or fields['xero_code'],
        'XeroName': decision.get('xero_name') or fields['xero_name'],
        'XeroType': decision.get('xero_type') or fields['xero_type'],
        'VantagepointCode': decision.get('vp_code') or _s(row.get('VantagepointCode')),
        'VantagepointName': vp_name,
        'VantagepointType': decision.get('vp_type') or _s(row.get('VantagepointType')),
        'XeroID': decision.get('xero_id') or fields['xero_id'],
        'Messages': _s(row.get('Messages')),
    }

    rowid = row.get('_rowid')
    if rowid is not None:
        collection_update(
            MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
            f"UPDATE {MAP_CHART_OF_ACCOUNTS_TABLE_NAME} SET "
            "XeroCode = ?, XeroName = ?, XeroType = ?, "
            "VantagepointCode = ?, VantagepointName = ?, VantagepointType = ?, "
            "XeroID = ? WHERE rowid = ?",
            [
                values['XeroCode'], values['XeroName'], values['XeroType'],
                values['VantagepointCode'], values['VantagepointName'],
                values['VantagepointType'], values['XeroID'], rowid,
            ],
        )
    else:
        # Defensive fallback (ensure_map_row should always provide the rowid for
        # a create/link/update/backfill outcome): upsert keyed by XeroID.
        _write_map_account_row(values)
    logger.info(
        "Reconciled map_chart_of_accounts row for Xero %s (action=%s, VP=%r)",
        values['XeroID'], action, values['VantagepointCode']
    )
    return values


def log_skip_method():
    """Workato skip outcome: nothing safe to do for this account."""
    fields = _xero_fields()
    logger.info(
        "Xero account %s (%s) is already in sync or has no safe action — "
        "skipping.", fields['xero_id'], fields['xero_code']
    )
    return None


# ---------------------------------------------------------------------------
# Error capture (return dict; do NOT raise — keeps the processor DAG SUCCESS so
# the dispatcher's sensor never sees a failed run and the gather operator can
# collect the error dict).
# ---------------------------------------------------------------------------
def capture_processor_error(xero_id, account_name, error_message):
    """Return an error dict the dispatcher aggregates (also logged)."""
    label = f"Chart of Accounts Xero AccountID={xero_id} ({account_name})"
    logger.error("%s - sync failed: %s", label, error_message)
    return {'error': f"{label} - sync failed: {error_message}"}

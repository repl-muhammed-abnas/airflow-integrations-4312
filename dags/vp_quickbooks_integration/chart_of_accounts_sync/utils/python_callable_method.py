"""
Python callable methods for QBO -> VP Chart of Accounts Sync.

Ports the Workato `chart_of_accounts` bundle (recipes
`014_503_psa_poll_quickbooks_upserted_account_code` and
`014_503_psa_sync_account_codes`, plus the two lookup tables) into
Python callables for the 3-DAG Airflow template (main -> dispatcher ->
processor).

Workato lookup tables:
  - account map (sync-state, keyed by QBOID) -> shared mapping_sync
    `map_account_code` S3 collection
  - account type map (static QBO Classification -> VP type) ->
    shared `common.tables.ACCOUNT_TYPE_MAP` product constant

The Workato main recipe runs a SQL join (QBO LEFT JOIN VP on
AcctNum=Account OR Name=Name, LEFT JOIN code-map on Id=QBOID, LEFT JOIN
type-map on Classification) and a per-account FOREACH with five outcomes:
add-to-map, link-in-map, create VP account, update VP account name, skip.
Here the dispatcher fetches the changed QBO accounts + the full VP account
list (slim index in conf); the processor reproduces the FOREACH decision
tree for one QBO account at a time.

Re-run safety is watermark-only — the dispatcher advances the watermark
only on a fully clean run, and the account_map's QBOID key keeps
already-linked accounts from being re-created.
"""
# pylint: disable=invalid-name,broad-exception-caught,too-many-return-statements
import logging
from airflow.models import Variable
import rail
# The shared collection helpers + table/column constants come from common so
# the S3 access logic and SQLite identifiers can't drift across integrations.
from vp_quickbooks_integration.common.python_callable_method import (
    collection_rows,
    collection_update,
    collection_upsert,
    unwrap_vp_response,
)
from vp_quickbooks_integration.common.tables import (
    MAP_ACCOUNT_CODE_TABLE_NAME,
    MAP_ACCOUNT_CODE_COLUMNS,
    MAP_ACCOUNT_CODE_UNIQUE_COLUMNS,
    ACCOUNT_TYPE_MAP,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Both Workato lookup tables are now sourced from shared code, NOT per-
# integration Airflow Variables: the account map (sync state) from the
# mapping_sync `map_account_code` S3 collection (see the collection helpers
# below), and the static QBO-classification -> VP-type map from
# common.tables.ACCOUNT_TYPE_MAP (a product-level constant, not tenant-specific).
# ---------------------------------------------------------------------------

# Optional global override for VP's maximum account-number length (Workato
# reads this from the VP System Formats; we fetch that too, but an explicit
# override Variable wins when present). Empty/absent => fall back to the
# System Formats value, else skip the length guard.
_MAX_ACCOUNT_LENGTH_VARIABLE_KEY = 'vp_qbo_chart_of_accounts_sync_max_account_length'

# VP System Formats entity filter for the account-number format. Configurable
# because the exact entity token is environment/version dependent; defaults to
# the conventional `account`. Used only to derive the max account length.
_SYSTEM_FORMATS_ENTITY_VARIABLE_KEY = (
    'vp_qbo_chart_of_accounts_sync_system_formats_entity'
)
_DEFAULT_SYSTEM_FORMATS_ENTITY = '?entity=account'

# Workato SQL `IFNULL(atm.VantagepointCode, 1)` — unmapped QBO Classifications
# default to VP type 1 (Asset).
_DEFAULT_VP_TYPE = '1'

# VP Account columns the Workato create/update payload always sends as blank
# (`=blank`). VP's /Accounts/ create+update handler references these columns
# and rejects the request when they are absent (observed: "Column:
# CashBasisAccount does not exist"). So we must send them — as empty strings —
# on both POST and PUT, exactly like the recipe.
_BLANK_VP_ACCOUNT_FIELDS = (
    'CashBasisAccount',
    'UnrealizedLossAccount',
    'UnrealizedGainAccount',
    'CashBasisRevaluation',
    'QBOAccountID',
)

# Candidate keys we probe in a VP System Formats record to discover the max
# account-number length. VP's CFGFormat payload field name is not contractual,
# so we look at the common spellings and use the first integer we find.
_MAX_LENGTH_CANDIDATE_KEYS = (
    'AccountLength', 'AcctLength', 'MaxLength', 'Length', 'Size', 'FieldLength',
)


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

    VP and QBO operators return numeric fields as ints/floats on the wire
    (e.g. the VP account `Type` comes back as `9`, not `"9"`), so a bare
    `.strip()` blows up with AttributeError. Coerce defensively; treat None
    as empty. `0`/`0.0` are preserved as `"0"`/`"0.0"` (not collapsed to '').
    """
    if value is None:
        return ''
    return str(value).strip()


def _qbo_fields():
    """Extract the QBO account identity from the processor's conf.

    The dispatcher passes the raw QBO Account record (spread into conf) plus
    the slim VP index. QBO field names: Id, AcctNum, Name, Classification.
    """
    conf = _conf()
    return {
        'qbo_id': _s(conf.get('Id')),
        'qbo_code': _s(conf.get('AcctNum')),
        'qbo_name': _s(conf.get('Name')),
        'classification': _s(conf.get('Classification')),
    }


# Collection access (read/write the shared map_account_code collection) uses the
# shared helpers in common.python_callable_method — `collection_rows` /
# `collection_update`, imported above and called directly.


def _account_insert(values, context=None):
    """INSERT one map_account_code row. Column order from common.tables so the
    SQLite identifiers can't drift; missing keys default to ''."""
    context = context or rail.get_current_context()
    columns = ', '.join(MAP_ACCOUNT_CODE_COLUMNS)
    placeholders = ', '.join(['?'] * len(MAP_ACCOUNT_CODE_COLUMNS))
    collection_update(
        MAP_ACCOUNT_CODE_TABLE_NAME,
        f"INSERT INTO {MAP_ACCOUNT_CODE_TABLE_NAME} ({columns}) "
        f"VALUES ({placeholders})",
        [values.get(col, '') for col in MAP_ACCOUNT_CODE_COLUMNS],
        context,
    )


def _write_map_account_row(values, context=None):
    """Upsert a map_account_code row keyed by (QBOID, VantagepointCode) via a
    single atomic S3UpsertCollectionOperator call.

    Workato/mapping_sync parity: map_account_code allows MULTIPLE VP codes per
    QBO account (fan-out), so the dedup key is the composite (QBOID,
    VantagepointCode) — never QBOID alone (that would clobber sibling rows for
    the same QBO account). This matches MAP_ACCOUNT_CODE_UNIQUE_COLUMNS, declared
    as a UNIQUE index by mapping_sync's init_mapping_collections, so ``INSERT ...
    ON CONFLICT(QBOID, VantagepointCode) DO UPDATE`` replaces the matching row's
    non-key columns in one atomic S3 commit — superseding the old
    DELETE-then-INSERT. Used only as the defensive fallback when an in-place
    rowid update isn't available; the normal path updates by rowid.
    """
    context = context or rail.get_current_context()
    collection_upsert(
        MAP_ACCOUNT_CODE_TABLE_NAME,
        MAP_ACCOUNT_CODE_UNIQUE_COLUMNS,
        {col: values.get(col, '') for col in MAP_ACCOUNT_CODE_COLUMNS},
        context,
    )


def map_qbo_type_to_vp(classification):
    """Map a QBO Classification to a VP account type code.

    Mirrors the Workato Account Type Map join (case-insensitive) with the
    `IFNULL(..., 1)` default, now sourced from the shared
    common.tables.ACCOUNT_TYPE_MAP product constant
    (QBO AccountType -> {'code', 'name'}) instead of a per-integration Airflow
    Variable. Unmapped classifications fall back to _DEFAULT_VP_TYPE ('1' Asset),
    matching the Workato IFNULL default.
    """
    wanted = (classification or '').strip().lower()
    if not wanted:
        return _DEFAULT_VP_TYPE
    for qbo_type, vp in ACCOUNT_TYPE_MAP.items():
        if qbo_type.strip().lower() == wanted:
            return str((vp or {}).get('code') or _DEFAULT_VP_TYPE)
    return _DEFAULT_VP_TYPE


# ---------------------------------------------------------------------------
# Dispatcher callables
# ---------------------------------------------------------------------------
def extract_account_list_method():
    """Extract the QBO account list from the QuickBooksAccountOperator result."""
    result = rail.result('get_recently_changed_accounts') or {}
    if not result.get('success'):
        logger.error(
            "QuickBooks account query failed: %s", result.get('error')
        )
        return []
    accounts = result.get('data') or []
    logger.info(
        "Found %d recently changed QuickBooks accounts", len(accounts)
    )
    return accounts


def build_vp_account_index_method():
    """Slim the full VP /Accounts list down to {Account, Name, Type} rows.

    This index rides in each processor's conf so the processor can reproduce
    the Workato `AcctNum=Account OR Name=Name` match without each child
    re-listing every VP account. VP chart-of-accounts records carry at least
    Account, Name, Type, Status; we keep only what the match + link need.
    """
    rows = unwrap_vp_response(
        rail.result('get_all_vp_accounts'), strict=True
    )
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
    """IfOperator test: did the QBO poll surface any changed accounts?"""
    return len(rail.result('extract_account_list') or []) > 0


def build_processor_dag_conf(item):
    """Build the per-account conf for the processor DAG.

    `item` is one QBO Account record. We spread it in (so the processor sees
    Id/AcctNum/Name/Classification directly) and attach the slim VP index plus
    the connections/customerId carried from the main DAG.
    """
    ctx_conf = rail.get_current_context()['dag_run'].conf or {}
    return {
        **item,
        'vp_account_index': rail.result('build_vp_account_index'),
        'connections': ctx_conf.get('connections'),
        'customerId': ctx_conf.get('customerId'),
    }


# ---------------------------------------------------------------------------
# Processor callables — the per-account decision tree (Workato FOREACH body)
# ---------------------------------------------------------------------------
def ensure_map_row_method():
    """Workato branch 1: add the QBO account to the code-map if absent.

    Idempotent — an existing row (already discovered, possibly linked) is left
    untouched so we never clobber an established VP link. Returns the row.
    """
    fields = _qbo_fields()
    qbo_id = fields['qbo_id']
    if not qbo_id:
        raise RuntimeError(
            "Processor dag_run.conf missing QBO account Id — cannot key the "
            "account_map. Refusing to process an account with no identity."
        )
    context = rail.get_current_context()
    rows = collection_rows(
        MAP_ACCOUNT_CODE_TABLE_NAME, MAP_ACCOUNT_CODE_COLUMNS,
        "QBOID = ?", [qbo_id], context,
    )
    if rows:
        # Already present (this run, a prior run, or mapping_sync). Leave as is
        # so we never clobber an established VP link / fan-out rows.
        logger.info(
            "QBO account %s already in map_account_code — leaving as is", qbo_id
        )
        return rows[0]
    blank_row = {
        'QBOCode': fields['qbo_code'],
        'QBOName': fields['qbo_name'],
        'QBOType': fields['classification'],
        'VantagepointCode': '',
        'VantagepointName': '',
        'VantagepointTypeRO': '',
        'QBOID': qbo_id,
    }
    _account_insert(blank_row, context)
    # Re-read so downstream tasks get the row WITH its sqlite _rowid (used by
    # link_account_in_map for an in-place update — Workato's update-by-EntryID).
    rows = collection_rows(
        MAP_ACCOUNT_CODE_TABLE_NAME, MAP_ACCOUNT_CODE_COLUMNS,
        "QBOID = ?", [qbo_id], context,
    )
    logger.info(
        "Added QBO account %s (%s) to map_account_code", qbo_id,
        fields['qbo_code']
    )
    return rows[0] if rows else blank_row


def match_vp_account_method():
    """Workato join: match this QBO account to a VP account by code OR name.

    Returns the two candidate VP rows (each {Account, Name, Type} or None):
        {'code_match': <by AcctNum=Account>, 'name_match': <by Name=Name>}
    `decide_action` applies the Workato precedence + the name-equality guard.
    """
    fields = _qbo_fields()
    qbo_code = fields['qbo_code']
    qbo_name = fields['qbo_name']
    index = _conf_value('vp_account_index', []) or []

    code_match = None
    name_match = None
    for vp in index:
        if not isinstance(vp, dict):
            continue
        vp_account = _s(vp.get('Account'))
        vp_name = _s(vp.get('Name'))
        if qbo_code and code_match is None and vp_account == qbo_code:
            code_match = vp
        if qbo_name and name_match is None and vp_name == qbo_name:
            name_match = vp
    logger.info(
        "VP match for QBO account %s (code=%r, name=%r): "
        "code_match=%s, name_match=%s",
        fields['qbo_id'], qbo_code, qbo_name,
        bool(code_match), bool(name_match)
    )
    return {'code_match': code_match, 'name_match': name_match}


def decide_action_method():
    """Reproduce the Workato per-account decision tree.

    Returns {'action', 'vp_code', 'vp_name', 'vp_type'} where action is one of
    'create' | 'update' | 'link' | 'skip'. The vp_* fields carry whatever the
    chosen action needs to write (VP record for link, target code for update,
    QBO-derived values + mapped type for create).
    """
    fields = _qbo_fields()
    qbo_id = fields['qbo_id']
    qbo_code = fields['qbo_code']
    qbo_name = fields['qbo_name']

    row = rail.result('ensure_map_row') or {}
    linked_code = _s(row.get('VantagepointCode'))
    match = rail.result('match_vp_account') or {}
    code_match = match.get('code_match')
    name_match = match.get('name_match')

    decision = {'action': 'skip', 'vp_code': '', 'vp_name': '', 'vp_type': ''}

    if linked_code:
        # Workato branch 3: already linked. Update the VP account's name when
        # the linked code still matches the QBO code AND the live VP name has
        # drifted from QBO's current name.
        if linked_code == qbo_code and code_match:
            current_vp_name = _s(code_match.get('Name'))
            if current_vp_name != qbo_name:
                decision.update(
                    action='update',
                    vp_code=linked_code,
                    vp_name=qbo_name,
                    vp_type=map_qbo_type_to_vp(fields['classification']),
                )
        logger.info(
            "decide_action(QBO %s): linked -> %s", qbo_id, decision['action']
        )
        return decision

    # Not yet linked (Workato branch 2).
    vp_match = code_match or name_match
    if vp_match:
        vp_name = _s(vp_match.get('Name'))
        # Branch 2a-i: only auto-link when the names are identical. Differing
        # names (2a-ii) are left for a human to reconcile -> skip.
        if vp_name == qbo_name:
            decision.update(
                action='link',
                vp_code=_s(vp_match.get('Account')),
                vp_name=vp_name,
                vp_type=_s(vp_match.get('Type')),
            )
        logger.info(
            "decide_action(QBO %s): unlinked, matched -> %s",
            qbo_id, decision['action']
        )
        return decision

    # Branch 2b-i: no VP match at all -> create a new VP account (needs a code).
    if qbo_code:
        decision.update(
            action='create',
            vp_code=qbo_code,
            vp_name=qbo_name,
            vp_type=map_qbo_type_to_vp(fields['classification']),
        )
    logger.info(
        "decide_action(QBO %s): unlinked, no match -> %s",
        qbo_id, decision['action']
    )
    return decision


def _action_is(action):
    """IfOperator test factory: is decide_action's chosen action == `action`?"""
    return (rail.result('decide_action') or {}).get('action') == action


def is_create_action():
    """IfOperator test: create a new VP account?"""
    return _action_is('create')


def is_update_action():
    """IfOperator test: update an existing VP account's name?"""
    return _action_is('update')


def is_link_action():
    """IfOperator test: link an existing VP account into the map (no VP write)?"""
    return _action_is('link')


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
    Formats response. Returns an int, or None when neither yields a number
    (in which case the caller skips the guard rather than blocking creates).
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
    """Workato pre-flight: refuse to create when the QBO code is too long.

    Raises (routing to error capture) with the Workato message when the QBO
    account number exceeds VP's max length. When the max length can't be
    determined, logs a warning and proceeds — degrading gracefully rather than
    blocking every create on an unknown System Formats shape.
    """
    fields = _qbo_fields()
    qbo_code = fields['qbo_code']
    qbo_name = fields['qbo_name']
    max_len = _extract_max_account_length()
    if max_len is None:
        logger.warning(
            "Could not determine VP max account length from System Formats or "
            "the '%s' override Variable — skipping the account-number length "
            "guard for this create.", _MAX_ACCOUNT_LENGTH_VARIABLE_KEY
        )
        return True
    if len(qbo_code) > max_len:
        raise RuntimeError(
            "Failed to add GLAccount to Vantagepoint from QuickBooks. "
            f"(account: {qbo_name}, number: {qbo_code}) Account number "
            f"exceeds maximum permitted number of characters as set in "
            f"Vantagepoint ({max_len})."
        )
    logger.info(
        "Account number %r within VP max length %s — OK", qbo_code, max_len
    )
    return True


# ---------------------------------------------------------------------------
# VP request-body builders
# ---------------------------------------------------------------------------
def build_create_account_body_method():
    """POST /Accounts/ body for a new VP GL account (Workato create step)."""
    fields = _qbo_fields()
    decision = rail.result('decide_action') or {}
    body = {
        'Account': fields['qbo_code'],
        'Name': fields['qbo_name'],
        'Status': 'A',
        'Type': decision.get('vp_type') or _DEFAULT_VP_TYPE,
        'Detail': '1',
    }
    for field in _BLANK_VP_ACCOUNT_FIELDS:
        body[field] = ''
    logger.info(
        "Built VP create body for account %s: %s", fields['qbo_code'], body
    )
    return body


def build_update_account_body_method():
    """PUT /Accounts/{code} body to refresh a VP account's name (Workato update)."""
    fields = _qbo_fields()
    decision = rail.result('decide_action') or {}
    body = {
        'Account': decision.get('vp_code') or fields['qbo_code'],
        'Name': fields['qbo_name'],
        'Status': 'A',
        'Type': decision.get('vp_type') or _DEFAULT_VP_TYPE,
        'Detail': '1',
    }
    for field in _BLANK_VP_ACCOUNT_FIELDS:
        body[field] = ''
    logger.info(
        "Built VP update body for account %s: %s", body['Account'], body
    )
    return body


# ---------------------------------------------------------------------------
# Map linking (Workato writes VP code/name/type into the code-map row)
# ---------------------------------------------------------------------------
def link_account_in_map_method():
    """Write the VP code/name/type from decide_action into the map row.

    Used both for the link branch (matched an existing VP account) and after a
    successful create (decide_action carries the new account's code/name/type).
    """
    fields = _qbo_fields()
    qbo_id = fields['qbo_id']
    decision = rail.result('decide_action') or {}
    vp_code = decision.get('vp_code') or ''
    vp_name = decision.get('vp_name') or fields['qbo_name']
    vp_type = decision.get('vp_type') or ''

    # ensure_map_row ran upstream and returned the row (with its sqlite _rowid).
    # Update that row IN PLACE by rowid — Workato's update_entry-by-EntryID —
    # leaving the QBO* columns untouched and never DELETE-ing by QBOID (which
    # would clobber sibling fan-out rows for the same QBO account).
    existing = rail.result('ensure_map_row') or {}
    rowid = existing.get('_rowid')
    if rowid is not None:
        collection_update(
            MAP_ACCOUNT_CODE_TABLE_NAME,
            f"UPDATE {MAP_ACCOUNT_CODE_TABLE_NAME} "
            "SET VantagepointCode = ?, VantagepointName = ?, "
            "VantagepointTypeRO = ? WHERE rowid = ?",
            [vp_code, vp_name, vp_type, rowid],
        )
    else:
        # Defensive fallback (ensure_map_row should always provide the rowid):
        # upsert keyed by the composite (QBOID, VantagepointCode).
        _write_map_account_row({
            'QBOCode': fields['qbo_code'],
            'QBOName': fields['qbo_name'],
            'QBOType': fields['classification'],
            'VantagepointCode': vp_code,
            'VantagepointName': vp_name,
            'VantagepointTypeRO': vp_type,
            'QBOID': qbo_id,
        })
    logger.info(
        "Linked QBO account %s to VP account %r in map_account_code",
        qbo_id, vp_code
    )
    return {
        'QBOCode': fields['qbo_code'],
        'QBOName': fields['qbo_name'],
        'QBOType': fields['classification'],
        'VantagepointCode': vp_code,
        'VantagepointName': vp_name,
        'VantagepointTypeRO': vp_type,
        'QBOID': qbo_id,
    }


def log_skip_method():
    """Workato branch 5: nothing to do for this account."""
    fields = _qbo_fields()
    logger.info(
        "QBO account %s (%s) is already in sync or has no safe action — "
        "skipping.", fields['qbo_id'], fields['qbo_code']
    )
    return None


# ---------------------------------------------------------------------------
# Error capture (return dict; do NOT raise — keeps the processor DAG SUCCESS so
# the dispatcher's sensor never sees a failed run and the gather operator can
# collect the error dict).
#
# Workato stamps the error into the lookup's col8, but that is an unlabeled,
# non-sticky column; the canonical map_account_code collection drops non-sticky
# columns (dense schema), so there's no Message column to write. The error is
# surfaced via the logger + the returned dict, which catch_processor_dag_error
# feeds to the dispatcher's gather/Fail path.
# ---------------------------------------------------------------------------
def capture_processor_error(qbo_id, account_name, error_message):
    """Return an error dict the dispatcher aggregates (also logged)."""
    label = f"Chart of Accounts QBO Id={qbo_id} ({account_name})"
    logger.error("%s - sync failed: %s", label, error_message)
    return {'error': f"{label} - sync failed: {error_message}"}



"""
Python callable methods for VP -> QBO Journal Entry Sync.

Ports the Workato recipes
`014_503_psa_poll_vantagepoint_posted_journal_entry`,
`014_503_psa_vantagepoint_journal_entry_exports_to_quickbooks`, and
`014_503_psa_get_project_clients` into Python callables for the 3-DAG
Airflow template (main -> dispatcher -> processor).

Lookup tables ported from Workato (READ-ONLY here — populated by mapping_sync):
  - account map -> shared mapping_sync `map_account_code` S3 collection
    (match VP account code on VantagepointCode -> QBOID/QBOName)
  - firm map    -> shared mapping_sync `map_firm` S3 collection
    (match VP FirmID on FirmID -> QBOID/Name; IsVendor ignored, per Workato)

Re-run safety is watermark-only (no posted-JE map). Validation gaps
raise from the processor; the dispatcher's per-child error capture
keeps the watermark behind on any failure so the same window re-polls
on the next run.
"""
# pylint: disable=invalid-name,broad-exception-caught,too-many-locals
import logging
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from urllib.parse import quote
import rail
# The shared collection helpers + table-name constants come from common so the
# S3 access logic and SQLite identifiers can't drift across integrations. JE is
# read-only against these collections, so it only needs `collection_rows`.
from vp_quickbooks_integration.common.python_callable_method import (
    collection_rows,
    unwrap_vp_response,
)
from vp_quickbooks_integration.common.tables import (
    MAP_ACCOUNT_CODE_TABLE_NAME,
    MAP_FIRM_TABLE_NAME,
)

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _vp_modules():
    """Lazy-load the rail submodules that trigger expensive operator-package
    discovery at import time.

    `from rail.hooks.vantagepoint_hook import VantagepointHook` and
    `from rail.operators.vantagepoint import utils` cause
    `rail.operators.__init__` to evaluate, which walks every operator
    subpackage on disk — including ones that may not exist on a given
    deployment (e.g. `xero_internal`). Each missing-path filesystem stat
    compounds and pushes DAG parse past Airflow's 30s
    `dagbag_import_timeout`.

    Wrapping the imports in this `@lru_cache(maxsize=1)` loader keeps them
    in one place (no duplication across consumers) and defers them to
    first call — by which point the DAG has already parsed. Subsequent
    calls hit the cache for free.
    """
    from rail.hooks.vantagepoint_hook import VantagepointHook
    from rail.operators.vantagepoint import utils as vp_utils
    return VantagepointHook, vp_utils


# Mirrors the Workato `014_503_psa_get_project_clients` helper recipe,
# which batches WBS1 codes in groups of 10 to keep VP /api/project
# request URLs under VP's length cap.
_PROJECT_CLIENTS_BATCH_SIZE = 10


# ---------------------------------------------------------------------------
# Generic helpers shared across dispatcher + processor
# ---------------------------------------------------------------------------
def _vp_conn_id():
    """Pull the VP connection id out of the running DAG's conf."""
    conf = rail.get_current_context()['dag_run'].conf
    connections = conf.get('connections') or {}
    return connections.get('vantagepoint')


def _conf_value(key, default=''):
    """Fetch a single key out of the current dag_run.conf."""
    conf = rail.get_current_context()['dag_run'].conf
    return conf.get(key) if conf.get(key) is not None else default


# ---------------------------------------------------------------------------
# Dispatcher: PSALedger PostDate watermark filter + grouping
# ---------------------------------------------------------------------------
def build_vp_psaledger_filter_method():
    """filterHash for the dispatcher's PSALedger poll.

    Two-sided `last <= PostDate < current` window so each poll claims a
    closed lower / open upper interval and the watermark advance is
    gap- and overlap-free.

    PSALedger rows do NOT carry a `ModDate` column (verified against a
    live VP response — the sibling `employee_sync_upsert` integration
    polls `/employee` which does have `ModDate`, but PSALedger uses
    `PostDate` for "when did this row become visible"). `TransDate` is
    the business date and can be backdated, so it's the wrong cursor.
    `PostDate` is the commit timestamp and never moves backwards.
    """
    timestamps = rail.result('prepare_sync_timestamps')
    last = quote(timestamps['last_sync_time'], safe='')
    current = quote(timestamps['current_sync_time'], safe='')
    gte = quote('>=', safe='')
    lt = quote('<', safe='')
    return (
        f"?filterHash[0][name]=PostDate"
        f"&filterHash[0][value]={last}"
        f"&filterHash[0][type]=datetime"
        f"&filterHash[0][opp]={gte}"
        f"&filterHash[0][condition]=AND"
        f"&filterHash[0][seq]=0"
        f"&filterHash[1][name]=PostDate"
        f"&filterHash[1][value]={current}"
        f"&filterHash[1][type]=datetime"
        f"&filterHash[1][opp]={lt}"
        f"&filterHash[1][seq]=1"
    )


def extract_journal_entries_list_method():
    """Group PSALedger line rows by (Period, PostSeq) — one entry per journal.

    The Workato `polling_PSALedger_updated` trigger already surfaces
    distinct (Period, PostSeq) pairs because Workato's polling layer
    dedupes for us. In Airflow we query the PSALedger directly and get
    line-level rows back, so we collapse them here.

    Returns a list of dicts shaped for the processor's dag_run.conf:
        [{'Period': ..., 'PostSeq': ..., 'FirstTransDate': ...,
          'RowCount': ...}, ...]
    """
    raw = rail.result('get_changed_psaledger_je_rows')
    rows = unwrap_vp_response(raw, strict=True)
    grouped = {}
    # PSALedger rows are required to carry Period + PostSeq — they are the
    # journal's primary key. Track any row that's missing them (or that
    # isn't a dict at all) so a non-zero `skipped` count surfaces in the
    # task log. Silently dropping would let the watermark advance past
    # journals that never made it to QBO, with no breadcrumb to debug.
    skipped = 0
    for row in rows:
        if not isinstance(row, dict):
            skipped += 1
            continue
        period = row.get('Period')
        post_seq = row.get('PostSeq')
        if period is None or post_seq is None:
            skipped += 1
            continue
        key = (str(period), str(post_seq))
        entry = grouped.get(key)
        if entry is None:
            grouped[key] = {
                'Period': str(period),
                'PostSeq': str(post_seq),
                'FirstTransDate': row.get('TransDate') or '',
                'RowCount': 1,
            }
        else:
            entry['RowCount'] += 1
    journals = list(grouped.values())
    summary = (
        f"Grouped {len(rows)} PSALedger JE rows into {len(journals)} "
        f"unique (Period, PostSeq) journal entries"
    )
    if skipped:
        summary += (
            f" — WARNING: skipped {skipped} malformed rows "
            "(missing Period/PostSeq or non-dict shape)"
        )
    logger.info("%s", summary)
    return journals


def check_if_journal_entries_exist_method():
    """IfOperator test: did the PSALedger poll surface any journals?"""
    return len(rail.result('extract_journal_entries_list') or []) > 0


# ---------------------------------------------------------------------------
# Processor: per-(Period, PostSeq) PSALedger fetch filter
# ---------------------------------------------------------------------------
def build_psaledger_period_postseq_filter_method():
    """Re-fetch all lines of this exact journal by (Period, PostSeq).

    Two AND'd filterHash clauses. We tried the simpler `?Period=X&
    PostSeq=Y` direct-query-param form earlier (matching the surface
    shape of Workato's connector inputs), but VP's PSALedger endpoint
    silently ignored those params and returned every JE row in the
    database — meaning the Workato connector clearly translates
    `Period`/`PostSeq` into filterHash internally before hitting VP.
    filterHash with `type=int` and exact-match `opp==` is the form VP
    actually honors. Period and PostSeq are integers on the wire
    (`202403`, `94`).
    """
    # Fail fast if the dispatcher (or a manual trigger) hands us a conf
    # without the journal identity. Without this guard, empty values flow
    # into the filterHash, VP returns zero rows, and
    # `extract_psaledger_lines_method` raises a misleading "No PSALedger
    # lines found" that looks like a VP data issue instead of a missing
    # config bug.
    period_value = _conf_value('Period')
    post_seq_value = _conf_value('PostSeq')
    if not period_value or not post_seq_value:
        raise RuntimeError(
            "Processor dag_run.conf missing Period or PostSeq — "
            f"got Period={period_value!r}, PostSeq={post_seq_value!r}. "
            "Refusing to query PSALedger with an empty journal identity."
        )
    period = quote(str(period_value), safe='')
    post_seq = quote(str(post_seq_value), safe='')
    eq = quote('=', safe='')
    return (
        f"?filterHash[0][name]=Period"
        f"&filterHash[0][value]={period}"
        f"&filterHash[0][type]=int"
        f"&filterHash[0][opp]={eq}"
        f"&filterHash[0][condition]=AND"
        f"&filterHash[0][seq]=0"
        f"&filterHash[1][name]=PostSeq"
        f"&filterHash[1][value]={post_seq}"
        f"&filterHash[1][type]=int"
        f"&filterHash[1][opp]={eq}"
        f"&filterHash[1][seq]=1"
    )


def extract_psaledger_lines_method():
    """Unwrap the PSALedger re-fetch into a list of line dicts."""
    raw = rail.result('get_psaledger_lines_for_journal')
    rows = unwrap_vp_response(raw, strict=True)
    lines = [r for r in rows if isinstance(r, dict)]
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    logger.info(
        "PSALedger journal (Period=%s, PostSeq=%s) has %d lines",
        period, post_seq, len(lines)
    )
    if not lines:
        raise RuntimeError(
            f"No PSALedger lines found for (Period={period}, "
            f"PostSeq={post_seq}) — this journal was surfaced by the "
            "dispatcher's poll but the per-journal re-fetch returned "
            "zero rows. Refusing to post an empty journal entry."
        )
    return lines


# Collection access is READ-ONLY here (resolve VP -> QBO refs from
# map_account_code / map_firm; never written). Uses the shared `collection_rows`
# helper in common.python_callable_method, imported above and called directly.


def load_lookup_tables_method():
    """Load both lookup tables from the shared mapping_sync S3 collections.

    Workato parity: the JE recipe does `get_entries` on Map Account + Map Firm
    (all rows) then SQL-joins them. We mirror that — load every row once and
    build the same in-memory dicts the downstream tasks already consume, so
    enrich/validate/build-body stay unchanged. account_map is keyed by VP
    account code (map_account_code.VantagepointCode); firm_map by VP FirmID
    (map_firm.FirmID).

    map_account_code / map_firm have no UNIQUE index on those key columns, so a
    key could (rarely) match more than one row. Workato neither filters
    (no IsVendor filter) nor dedups — its SQL join would fan out — but a single-
    value dict can't fan out (and a fanned-out JE line would be wrong), so we
    keep the FIRST row per key and log a warning when duplicates exist, leaving
    the data anomaly visible.
    """
    context = rail.get_current_context()

    account_map = {}
    for r in collection_rows(
        MAP_ACCOUNT_CODE_TABLE_NAME,
        ['VantagepointCode', 'QBOID', 'QBOName'],
        '1 = 1', [], context,
    ):
        code = (r.get('VantagepointCode') or '').strip()
        if not code:
            continue
        if code in account_map:
            logger.warning(
                "Duplicate map_account_code row for VantagepointCode %r — "
                "keeping the first.", code
            )
            continue
        account_map[code] = {
            'QBOID': r.get('QBOID') or '',
            'QBOName': r.get('QBOName') or '',
        }

    firm_map = {}
    for r in collection_rows(
        MAP_FIRM_TABLE_NAME,
        ['FirmID', 'QBOID', 'Name'],
        '1 = 1', [], context,
    ):
        firm_id = (r.get('FirmID') or '').strip()
        if not firm_id:
            continue
        if firm_id in firm_map:
            logger.warning(
                "Duplicate map_firm row for FirmID %r — keeping the first.",
                firm_id
            )
            continue
        # IsVendor is intentionally NOT loaded: Workato parity sets the JE
        # EntityRef by QBOID only and ignores IsVendor (QBO infers the type).
        firm_map[firm_id] = {
            'QBOID': r.get('QBOID') or '',
            'Name': r.get('Name') or '',
        }

    logger.info(
        "Loaded lookup tables from mapping_sync S3 collections: "
        "account_map=%d entries, firm_map=%d entries",
        len(account_map), len(firm_map)
    )
    return {'account_map': account_map, 'firm_map': firm_map}


# ---------------------------------------------------------------------------
# WBS1 -> project/client lookup (Workato helper recipe `get_project_clients`)
# ---------------------------------------------------------------------------
def extract_unique_wbs1_method():
    """Dedupe non-empty WBS1 values from the PSALedger lines."""
    lines = rail.result('extract_psaledger_lines') or []
    unique = sorted({
        (line.get('WBS1') or '').strip()
        for line in lines
        if (line.get('WBS1') or '').strip()
    })
    logger.info("Unique WBS1 codes in this journal: %d", len(unique))
    return unique


def _build_wbs1_batch_filter(batch):
    """Build a filterHash query string for one batch of WBS1 codes.

    Mirrors the Workato `get_project_clients` recipe block 9 (line 367)
    exactly — only the two `name` and `value` clauses per WBS1, joined
    by `&`. No explicit `type`, `opp`, `seq`, or `condition` params:
    Workato omits them and VP defaults make repeated same-field
    filterHash clauses behave as OR (which is what we need — "find any
    project whose WBS1 is in this batch"). Adding an explicit `seq` or
    `condition` could in principle flip the semantics to AND and return
    zero matches.
    """
    parts = ['fieldFilter=WBSNumber,WBS1,WBS2,WBS3,Name,ClientID']
    for index, wbs1 in enumerate(batch):
        parts.append(f"filterHash[{index}][name]=WBS1")
        parts.append(f"filterHash[{index}][value]={quote(wbs1, safe='')}")
    return '?' + '&'.join(parts)


def get_project_clients_from_vp_method():
    """Fetch WBS1 -> ClientID rows from VP, batched 10 per request.

    Workato's helper recipe `014_503_psa_get_project_clients` iterates
    batches of 10 WBS1 codes through VP `/api/project`. A single
    `VantagepointProjectOperator` task can't loop — so we use the
    `VantagepointHook` directly here, replicating the per-batch call
    pattern and accumulating results.

    Returns a flat list of project dicts (WBS1, WBS2, WBS3, Name, ClientID).
    """
    unique_wbs1 = rail.result('extract_unique_wbs1') or []
    if not unique_wbs1:
        logger.info("No WBS1 codes — skipping VP /api/project lookup")
        return []

    vp_conn_id = _vp_conn_id()
    if not vp_conn_id:
        raise RuntimeError(
            "No `connections.vantagepoint` in dag_run.conf — cannot "
            "query VP /api/project for project-client resolution."
        )
    VantagepointHook, vp_utils = _vp_modules()
    vp_client = VantagepointHook(vp_conn_id)

    log = rail.get_current_context()['task'].log
    accumulated = []
    batch_size = _PROJECT_CLIENTS_BATCH_SIZE
    for batch_idx in range(0, len(unique_wbs1), batch_size):
        batch = unique_wbs1[batch_idx:batch_idx + batch_size]
        filters = _build_wbs1_batch_filter(batch)
        log.info(
            "Fetching VP /api/project batch %d (%d WBS1 codes)",
            (batch_idx // batch_size) + 1, len(batch),
        )
        raw = vp_utils.execute_api_request(
            vp_client=vp_client,
            endpoint='/project',
            request_method='GET',
            filters=filters,
            request_body=None,
            pagination=True,
            log=log,
            base_path='/api',
        )
        rows = unwrap_vp_response(raw, strict=False)
        accumulated.extend(r for r in rows if isinstance(r, dict))

    logger.info(
        "VP /api/project returned %d project rows for %d unique WBS1 codes",
        len(accumulated), len(unique_wbs1)
    )
    return accumulated


# Delimiter used to fold the composite (WBS1, WBS2, WBS3) key into a
# single string for XCom JSON storage. Airflow's XCom serializer rejects
# tuple keys (JSON only allows string keys). The Unit Separator control
# char (\x1f) is the conventional choice for joining fields that
# themselves may contain printable punctuation — WBS codes are
# alphanumeric + dots so this is collision-free in practice.
_WBS_KEY_DELIMITER = '\x1f'


def _wbs_key(wbs1, wbs2, wbs3):
    """Build the project-index dict key from three WBS components."""
    return (
        f"{(wbs1 or '').strip()}"
        f"{_WBS_KEY_DELIMITER}{(wbs2 or '').strip()}"
        f"{_WBS_KEY_DELIMITER}{(wbs3 or '').strip()}"
    )


def build_project_client_index_method():
    """Index the project-client rows by WBS1/WBS2/WBS3 composite key.

    Workato's main recipe block 13 uses this composite key for the
    `LEFT JOIN debit.WBS1 = proj.WBS1 AND debit.WBS2 = proj.WBS2 AND
    debit.WBS3 = proj.WBS3` step. VP project rows for parent WBS levels
    often carry empty strings for the deeper WBS2/WBS3 slots — we
    normalize via `.strip()` so dict-lookups are exact-match against
    the PSALedger line WBS values.

    The key is a single delimiter-joined string (not a tuple) because
    Airflow XCom serializes via JSON, which rejects tuple keys.
    """
    rows = rail.result('get_project_clients_from_vp') or []
    index = {}
    for row in rows:
        key = _wbs_key(row.get('WBS1'), row.get('WBS2'), row.get('WBS3'))
        index[key] = {
            'ClientID': (row.get('ClientID') or '').strip(),
            'Name': row.get('Name') or '',
        }
    logger.info("Built project-client index with %d entries", len(index))
    return index


# ---------------------------------------------------------------------------
# Enrich + firm fallback + validate
# ---------------------------------------------------------------------------
def _line_amount_decimal(line):
    """Parse line.Amount as a Decimal (PSALedger amounts are strings)."""
    raw = line.get('Amount')
    if raw is None or raw == '':
        return Decimal('0')
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return Decimal('0')


def enrich_lines_method():
    """Attach QBO account + firm refs to each PSALedger line.

    Mirrors the Workato SQL JOIN at recipe lines 4572 (debit) and 4761
    (credit) — uses our two lookup tables and the project-client index.
    `QBOFirmID` may remain blank when the firm is missing from the map;
    `firm_fallback_from_vp_method` runs next and tries to populate it
    from the VP firm record itself (Workato block 19-21).
    """
    lines = rail.result('extract_psaledger_lines') or []
    lookups = rail.result('load_lookup_tables') or {}
    account_map = lookups.get('account_map') or {}
    firm_map = lookups.get('firm_map') or {}
    project_index = rail.result('build_project_client_index') or {}

    enriched = []
    for line in lines:
        account_code = (line.get('Account') or '').strip()
        account_row = account_map.get(account_code) or {}

        wbs_key = _wbs_key(
            line.get('WBS1'), line.get('WBS2'), line.get('WBS3'),
        )
        project_row = project_index.get(wbs_key) or {}
        client_id = project_row.get('ClientID') or ''
        firm_row = firm_map.get(client_id) if client_id else {}
        firm_row = firm_row or {}

        enriched.append({
            **line,
            '_AccountCode': account_code,
            '_QBOAccountID': account_row.get('QBOID') or '',
            '_QBOAccountName': account_row.get('QBOName') or '',
            '_ClientID': client_id,
            '_QBOFirmID': firm_row.get('QBOID') or '',
            # `_FirmName` is carried purely for VALIDATION ERROR CONTEXT (the
            # QBO Entity is value-only now — no name — per Workato parity). We
            # deliberately do NOT fall back to project_row.Name here — the
            # project name and the firm name are different concepts, and using
            # the project name would mislead error messages. When firm_map
            # misses, `_FirmName` stays empty and `firm_fallback_from_vp_method`
            # populates it from a live VP /firm/{ClientID} fetch (Workato blocks
            # 20/26 — VP is the system of record for firm display names).
            '_FirmName': firm_row.get('Name') or '',
        })
    logger.info(
        "Enriched %d lines with account + firm + project refs", len(enriched)
    )
    return enriched


def firm_fallback_from_vp_method():
    """Annotate enriched lines with the VP firm Name for cleaner error messages.

    Workato main recipe blocks 19-21 (debit) and 25-27 (credit) fetch
    `/firm/{ClientID}` from VP whenever the firm-map JOIN returns a
    blank QBOID but the line has a non-empty ClientID. The fetched
    record is used ONLY to read `firm.Name`, which then goes into the
    CompoundError message:
        "Vantagepoint firm {Name} not matched to a QuickBooks firm"

    The recipe does NOT use the VP firm's `QBOID` field (even though
    VP firm records do carry one) as a fallback for the missing
    lookup-table value — the lookup table is the canonical source and
    a miss there is a hard failure. We mirror that: we attach the
    Name onto the line as `_FirmName` (for the validation step to use)
    but never touch `_QBOFirmID`.

    Cached by ClientID so repeated lines for the same firm don't
    re-fetch.
    """
    enriched = rail.result('enrich_lines') or []
    needing_lookup = {
        line['_ClientID']
        for line in enriched
        if line.get('_ClientID') and not line.get('_QBOFirmID')
    }
    if not needing_lookup:
        logger.info("No unmapped firms — skipping VP firm name lookup.")
        return enriched

    vp_conn_id = _vp_conn_id()
    if not vp_conn_id:
        raise RuntimeError(
            "No `connections.vantagepoint` in dag_run.conf — cannot "
            "fetch VP firm names for error annotation."
        )
    VantagepointHook, vp_utils = _vp_modules()
    vp_client = VantagepointHook(vp_conn_id)
    log = rail.get_current_context()['task'].log

    name_cache = {}
    for client_id in sorted(needing_lookup):
        encoded = quote(str(client_id), safe='')
        log.info("VP firm name lookup: fetching /firm/%s", client_id)
        try:
            raw = vp_utils.execute_api_request(
                vp_client=vp_client,
                endpoint=f'/firm/{encoded}',
                request_method='GET',
                filters='',
                request_body=None,
                pagination=False,
                log=log,
                base_path='/api',
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "VP firm name lookup failed for ClientID=%s: %s",
                client_id, exc,
            )
            name_cache[client_id] = ''
            continue
        record = raw
        if isinstance(raw, list) and raw:
            record = raw[0]
        elif isinstance(raw, dict):
            for key in ('Body', 'body', 'rows', 'data'):
                value = raw.get(key)
                if isinstance(value, list) and value:
                    record = value[0]
                    break
                if isinstance(value, dict):
                    record = value
                    break
        if not isinstance(record, dict):
            record = {}
        name_cache[client_id] = (record.get('Name') or '').strip()

    annotated_count = 0
    for line in enriched:
        if line.get('_QBOFirmID') or not line.get('_ClientID'):
            continue
        firm_name = name_cache.get(line['_ClientID']) or ''
        if firm_name:
            # VP is the system of record for the firm's display name.
            # Always overwrite — even if firm_map happened to carry a Name
            # (col4) that enrich_lines set first. Stale firm_map.Name
            # entries would otherwise mislead the validation error.
            line['_FirmName'] = firm_name
            annotated_count += 1
    logger.info(
        "VP firm name lookup: probed %d firms, annotated %d lines with firm "
        "names for error context", len(needing_lookup), annotated_count
    )
    return enriched


def validate_enriched_lines_method():
    """Walk the enriched lines and raise on any mapping gap.

    Mirrors Workato's CompoundError accumulation (blocks 17-18, 19-21,
    23-24, 25-27) — surface every gap in one message so operators can
    fix all of them in a single map edit, not one at a time. Scoped per
    journal-entry (this processor's (Period, PostSeq)), so a bad
    journal does not poison sibling processors in the same poll window.
    """
    enriched = rail.result('firm_fallback_from_vp') or []
    if not enriched:
        raise RuntimeError("Refusing to post a journal with zero lines.")

    errors = []
    debit_total = Decimal('0')
    credit_total = Decimal('0')
    for line in enriched:
        amount = _line_amount_decimal(line)
        if amount > 0:
            debit_total += amount
        elif amount < 0:
            credit_total += -amount

        # Workato block 17/23: "Vantagepoint account {Account} not matched
        # to a QuickBooks account". Workato OR's into a single message, but
        # the two failure modes are operationally different:
        #   - QBOID missing  → the JOIN missed (no row for this VP code in
        #                       account_map). Ops should ADD a row.
        #   - QBOID present but QBOName missing → row exists but is
        #                       incomplete. Ops should EDIT the QBOName
        #                       column on the existing row.
        # Distinguishing them lands the operator's fix on the right column
        # the first time. `elif` keeps it one error per line (no double-count).
        if not line.get('_QBOAccountID'):
            errors.append(
                f"Vantagepoint account {line.get('_AccountCode')} "
                "not matched to a QuickBooks account "
                f"(line PKey={line.get('PKey')})."
            )
        elif not line.get('_QBOAccountName'):
            errors.append(
                f"Account map row for {line.get('_AccountCode')} is missing "
                f"QBOName (line PKey={line.get('PKey')})."
            )
        # Workato block 21/27: "Vantagepoint firm {Name} not matched to a
        # QuickBooks firm" — Name comes from the VP firm fetch in
        # blocks 20/26 (annotated onto the line as `_FirmName` in
        # `firm_fallback_from_vp_method`). Fall back to ClientID when
        # the VP fetch didn't return a name.
        if line.get('_ClientID') and not line.get('_QBOFirmID'):
            firm_label = line.get('_FirmName') or line['_ClientID']
            errors.append(
                f"Vantagepoint firm {firm_label} "
                "not matched to a QuickBooks firm "
                f"(line PKey={line.get('PKey')})."
            )

    if debit_total != credit_total:
        errors.append(
            f"Journal is not balanced: debits={debit_total}, "
            f"credits={credit_total}. QuickBooks would reject this "
            "POST — refusing to send."
        )

    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')
    if errors:
        raise RuntimeError(
            f"Journal entry (Period={period}, PostSeq={post_seq}) "
            f"failed validation:\n  - " + "\n  - ".join(errors)
        )
    logger.info(
        "Validation passed for (Period=%s, PostSeq=%s): %d lines, "
        "debits=%s, credits=%s",
        period, post_seq, len(enriched), debit_total, credit_total
    )
    return enriched


# ---------------------------------------------------------------------------
# Build the final QBO JournalEntry body
# ---------------------------------------------------------------------------
def _line_description(line):
    """QBO line Description from Workato recipe lines 7360 / 7376 — Desc1 only.

    The Workato main recipe explicitly writes only `Desc1` to the QBO line
    Description; `Desc2` is fetched into the PSALedger row dict but never
    forwarded. Keeping the same behavior here for fidelity.
    """
    return (line.get('Desc1') or '').strip()


def _normalize_txn_date(raw):
    """PSALedger TransDate is typically ISO date or `YYYY-MM-DDTHH:MM:SS`.

    QuickBooks JournalEntry TxnDate expects a bare `YYYY-MM-DD`.
    """
    if not raw:
        return ''
    text = str(raw)
    if 'T' in text:
        return text.split('T', 1)[0]
    return text[:10]


def build_journal_entry_body_method():
    """Assemble the final QBO JournalEntry payload.

    Workato's main recipe blocks 31 split lines into Entry / OppositeEntry
    (a Workato-specific shape). The actual QBO API and the RAIL
    `QuickBooksJournalEntryOperator` use a single flat `Line[]` array
    where each line carries its own PostingType. We emit all debits
    followed by all credits in that one array.
    """
    enriched = rail.result('validate_enriched_lines') or []
    period = _conf_value('Period')
    post_seq = _conf_value('PostSeq')

    debit_lines = []
    credit_lines = []
    first_debit_txn_date = ''

    for line in enriched:
        amount = _line_amount_decimal(line)
        if amount == 0:
            continue

        is_debit = amount > 0
        # Send the QBO Amount as a Decimal-derived STRING, not a float.
        # `validate_enriched_lines_method` proves debit_total == credit_total
        # exactly using Decimal arithmetic. Converting to float here would
        # reintroduce binary-floating-point rounding errors that the
        # validation just ruled out — a JE we proved balanced could be
        # rejected by QBO as unbalanced (or accepted with a drifted total).
        # QBO accepts string-formatted decimals on Numeric fields.
        abs_amount = str(abs(amount))
        entry_detail = {
            'PostingType': 'Debit' if is_debit else 'Credit',
            'AccountRef': {
                'value': line['_QBOAccountID'],
                'name': line.get('_QBOAccountName') or '',
            },
        }
        if line.get('_QBOFirmID'):
            # Workato parity: the JE recipe sets EntityRef.value (the QBO entity
            # Id) ONLY — no Type and no name. QBO infers Vendor vs Customer from
            # the entity Id, and resolves the display name itself.
            entry_detail['Entity'] = {
                'EntityRef': {
                    'value': line['_QBOFirmID'],
                },
            }

        qbo_line = {
            'Amount': abs_amount,
            'DetailType': 'JournalEntryLineDetail',
            'Description': _line_description(line),
            'JournalEntryLineDetail': entry_detail,
        }
        if is_debit:
            debit_lines.append(qbo_line)
            if not first_debit_txn_date:
                first_debit_txn_date = _normalize_txn_date(
                    line.get('TransDate')
                )
        else:
            credit_lines.append(qbo_line)

    body = {
        'TxnDate': first_debit_txn_date,
        'DocNumber': f"{period}-{post_seq}",
        'PrivateNote': f"VP PSALedger JE {period}-{post_seq}",
        'Line': debit_lines + credit_lines,
    }
    logger.info(
        "Built QBO JournalEntry body for %s-%s: %d debit lines, "
        "%d credit lines, TxnDate=%s",
        period, post_seq, len(debit_lines), len(credit_lines),
        first_debit_txn_date
    )
    return body


# ---------------------------------------------------------------------------
# Error capture (return dict; do NOT raise — keeps the processor DAG SUCCESS
# so the dispatcher's WaitForDagRunsSensor never sees a failed run and
# GatherResultsFromDagRunsOperator can collect the error dict.)
# ---------------------------------------------------------------------------
def capture_processor_error(period, post_seq, error_message):
    """Return an error dict the dispatcher can aggregate."""
    label = f"Journal Entry (Period={period}, PostSeq={post_seq})"
    return {
        'error': f"{label} - sync failed: {error_message}"
    }



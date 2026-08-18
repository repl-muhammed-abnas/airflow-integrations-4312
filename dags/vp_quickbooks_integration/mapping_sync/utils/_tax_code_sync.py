"""
Tax code mapping sync (sections R+S+T from the pre-split file).

Operator-driven port of the Workato recipe `014-503 PSA Sync Tax Codes`.
The DAG (`map_tax_code_dag`) stages run-local collection tables and runs
the recipe's two `query_list` SQL steps via QueryCollectionOperator
(mirroring abbviemst `time_export_child`); this module supplies the
collection sources, the ported SQL, and the step-18 foreach that POST/PUTs
VP tax codes and writes map_tax_code. See section banner comments below
for fix-log references (MAP_TAX_CODE_SYNC_FIX_LOG.md #1-#6).

Public surface (re-exported via `python_callable_method.py`):
    sync_qbo_tax_codes_to_vp          — step 18 foreach (PythonOperator)
    build_qbo_tax_rates_staging       — CreateCollectionOperator source (step 12)
    prepare_vp_tax_codes_staging      — CreateCollectionOperator source (step 14)
    read_map_tax_code_for_staging     — CreateCollectionOperator source (step 11/13)
    TAX_GROUP_IDS_SQL                 — QueryCollectionOperator query (step 15)
    COMPILE_TAX_CODES_SQL             — QueryCollectionOperator query (step 17)

Also exports for internal mapping_sync use (flatten helper consumed
by tests / debugging):
    flatten_qbo_tax_rates
"""
import rail

# Shared helpers still live in `python_callable_method.py` during the
# staged split (they move to `_shared.py` in a follow-up extraction).
from vp_quickbooks_integration.mapping_sync.utils._shared import (
    _extract_qbo_records,
    _filter_none,
)
from vp_quickbooks_integration.common.tables import (
    MAP_TAX_CODE_TABLE_NAME,
    MAP_TAX_CODE_UNIQUE_COLUMNS,
)
from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig


# ===========================================================================
# TAX CODE MAPPING — schema (9 sticky Workato columns; col5 gap dropped)
# ===========================================================================
# QBOCodeName       = TaxCode.Name
# QBORateName       = TaxRate.Name
# QBOCodeID         = TaxCode.Id
# VantagepointCode  = VP tax code identifier
# Rate              = effective rate %, e.g. '7.25'
# TaxTypeApplicable = e.g. 'TaxOnAmount'
# QBORateID         = TaxRate.Id
# IsTaxGroup        = 'Y' / 'N'
# TaxOn             = 'Sales' / 'Purchase'
#
# Match key for upsert: (QBOCodeID, QBORateID).

# MAP_TAX_CODE_TABLE_NAME + MAP_TAX_CODE_COLUMNS in utils/tables.py.


# ===========================================================================
# TAX CODE MAPPING — QBO tax-rate flattening (mirrors Workato `FlattenTaxRates`)
# ===========================================================================

def _index_qbo_tax_rates_by_id(qbo_tax_rates):
    """Build an Id → TaxRate dict for fast lookups during flattening."""
    return {
        str(rate.get('Id')): rate
        for rate in (qbo_tax_rates or [])
        if rate.get('Id') is not None
    }


def _effective_rate_for_tax_rate(qbo_tax_rate):
    """Pick the current effective RateValue from a QBO TaxRate.

    Strategy (mirrors Workato `FlattenTaxRates`, recipe step 4 JS):
    use the top-level `RateValue` when present, and ONLY when it is
    absent/falsy fall back to scanning `EffectiveTaxRate` for an entry
    whose [EffectiveDate, EndDate] window contains today. The recipe
    checks RateValue first (`component.Rate = taxRate.RateValue; if
    (!taxRate.RateValue && taxRate.EffectiveTaxRate) {...}`); most
    tenants carry a single top-level rate without a date range.
    """
    from datetime import datetime, timezone  # pylint: disable=import-outside-toplevel
    if not qbo_tax_rate:
        return None
    # RateValue first — matches the recipe's order of preference.
    rate_value = qbo_tax_rate.get('RateValue')
    if rate_value:
        return rate_value
    today = datetime.now(timezone.utc).date()
    for entry in qbo_tax_rate.get('EffectiveTaxRate') or []:
        eff = entry.get('EffectiveDate')
        end = entry.get('EndDate')
        try:
            eff_d = datetime.strptime(
                eff[:10], '%Y-%m-%d').date() if eff else None
            end_d = datetime.strptime(
                end[:10], '%Y-%m-%d').date() if end else None
        except (ValueError, TypeError):
            continue
        if eff_d and eff_d > today:
            continue
        if end_d and end_d < today:
            continue
        entry_rate = entry.get('RateValue')
        if entry_rate is not None:
            return entry_rate
    # Final fallback: whatever the top-level RateValue was (may be 0/None).
    return rate_value


def _flatten_tax_rate_list(qbo_tax_code, tax_rate_index, list_key, tax_on):
    """Expand a single TaxRateDetail list (Sales or Purchase) into rows.

    Each entry in `<list_key>.TaxRateDetail` carries a `TaxRateRef.value`
    pointing at a TaxRate. We join those by Id, compute the current
    effective rate, and emit one row per (TaxCode, TaxRate) pair.
    """
    detail_list = (qbo_tax_code.get(list_key) or {}).get('TaxRateDetail') or []
    rows = []
    for detail in detail_list:
        rate_ref = detail.get('TaxRateRef') or {}
        rate_id = rate_ref.get('value')
        if not rate_id:
            continue
        tax_rate = tax_rate_index.get(str(rate_id))
        if not tax_rate:
            continue
        effective_rate = _effective_rate_for_tax_rate(tax_rate)
        rows.append({
            'CodeName': qbo_tax_code.get('Name') or '',
            'CodeID': str(qbo_tax_code.get('Id') or ''),
            'RateName': tax_rate.get('Name') or rate_ref.get('name') or '',
            'RateID': str(rate_id),
            'Rate': '' if effective_rate is None else str(effective_rate),
            'TaxTypeApplicable': detail.get('TaxTypeApplicable') or '',
            'IsTaxGroup': 'Y' if qbo_tax_code.get('TaxGroup') else 'N',
            'TaxOn': tax_on,
            # Additional fields to match Workato
            'Description': qbo_tax_code.get('Description') or '',
            'SpecialTaxType': tax_rate.get('SpecialTaxType') or '',
            'DisplayType': tax_rate.get('DisplayType') or '',
            'Active': qbo_tax_code.get('Active', True),
            'IsSalesTax': tax_on == 'Sales',
            'IsPurchaseTax': tax_on == 'Purchase',
        })
    return rows


def flatten_qbo_tax_rates(qbo_tax_codes, qbo_tax_rates):
    """Flatten QBO TaxCodes + TaxRates into one row per (TaxCode, TaxRate)
    component. Mirrors the Workato `FlattenTaxRates` JS function.

    Each QBO TaxCode can carry both a SalesTaxRateList and a
    PurchaseTaxRateList. We expand both, tagging each row with `TaxOn`
    ('Sales' or 'Purchase'). The resulting list is what we store in
    map_tax_code — one row per rate component.
    """
    tax_rate_index = _index_qbo_tax_rates_by_id(qbo_tax_rates)
    flattened = []
    for qbo_tax_code in qbo_tax_codes or []:
        flattened.extend(_flatten_tax_rate_list(
            qbo_tax_code, tax_rate_index,
            'SalesTaxRateList', 'Sales',
        ))
        flattened.extend(_flatten_tax_rate_list(
            qbo_tax_code, tax_rate_index,
            'PurchaseTaxRateList', 'Purchase',
        ))
    return flattened


# ===========================================================================
# TAX CODE MAPPING — collection staging + recipe SQL (Workato steps 10-17)
# ===========================================================================
# Faithful, operator-driven port of the recipe's smart-list pipeline. The
# DAG stages three run-local collection tables and runs the recipe's two
# `query_list` SQL steps via rail.QueryCollectionOperator (mirroring the
# abbviemst time_export_child pattern):
#
#   Step 10  List VP tax codes        -> fetch_vp_tax_codes (VP GET)
#   Step 12  create_list QBOTaxRates  -> collection `qbo_tax_rates`
#            (source = build_qbo_tax_rates_staging)
#   Step 13/11 Tax Code Map           -> collection `tax_code_map`
#            (source = read_map_tax_code_for_staging — copies the S3 map)
#   Step 14  Vantagepoint Tax Codes   -> collection `vp_tax_codes`
#            (source = prepare_vp_tax_codes_staging)
#   Step 15  IsTaxGroup query_list    -> collection `tax_group_ids`
#            (TAX_GROUP_IDS_SQL)
#   Step 17  Compile from all sources -> collection `compiled_tax_codes`
#            (COMPILE_TAX_CODES_SQL)
#   Step 18  foreach                  -> sync_qbo_tax_codes_to_vp
#
# IsTaxGroup is derived from rate count (NOT the QBO `TaxGroup` flag):
# a CodeID is a group when it has >1 rate on the same side. The compile
# JOIN keeps Workato's full `vtc` fan-out — a QBO rate that name-matches
# several VP tax codes becomes several map rows (e.g. NO TAX PURCHASE →
# VP 6,7,8,9). Idempotency is via the (QBOCodeID, QBORateID,
# VantagepointCode) unique key, not by collapsing the fan-out. NOTE: this
# requires a clean VP tenant — a tenant polluted with many duplicate-named
# VP tax codes will fan out proportionally.

# Run-local collection table names (must match the DAG task `name=`s).
QBO_TAX_RATES_COLLECTION = 'qbo_tax_rates'
VP_TAX_CODES_COLLECTION = 'vp_tax_codes'
TAX_CODE_MAP_COLLECTION = 'tax_code_map'
TAX_GROUP_IDS_COLLECTION = 'tax_group_ids'
COMPILED_TAX_CODES_COLLECTION = 'compiled_tax_codes'

# Explicit column lists for the CreateCollectionOperator stages — required
# because the source list can be empty (e.g. an empty map on first sync),
# in which case columns cannot be inferred.
QBO_TAX_RATES_STAGING_COLUMNS = [
    'CodeName', 'CodeID', 'RateName', 'RateID', 'Rate', 'TaxTypeApplicable',
    'TaxOn', 'IsSalesTax', 'IsPurchaseTax', 'Description', 'SpecialTaxType',
    'DisplayType', 'Active',
]
VP_TAX_CODES_STAGING_COLUMNS = ['Code', 'Description']
TAX_CODE_MAP_STAGING_COLUMNS = [
    'QBOCodeName', 'QBORateName', 'QBOCodeID', 'VantagepointCode', 'Rate',
    'TaxTypeApplicable', 'QBORateID', 'IsTaxGroup', 'TaxOn', 'EntryID',
]

# Step 15 — IsTaxGroup (recipe `GROUP BY CodeID, IsSalesTax, IsPurchaseTax
# HAVING COUNT(*) > 1`, then GROUP BY CodeID). We group on TaxOn, which is
# 1:1 with (IsSalesTax, IsPurchaseTax) in the staged data and avoids
# boolean-storage ambiguity.
TAX_GROUP_IDS_SQL = (
    "SELECT CodeID, IsTaxGroup FROM ("
    "  SELECT CodeID, 'Y' AS IsTaxGroup FROM qbo_tax_rates"
    "  GROUP BY CodeID, TaxOn HAVING COUNT(*) > 1"
    ") GROUP BY CodeID"
)

# Step 17 — Compile Tax Codes from all sources. Faithful port of the recipe's
# 4-way LEFT JOIN: tcm (existing map, names-or-ids), vtc (existing VP tax
# codes, name-or-mapped-code), tgi (tax-group flag by CodeID). The vtc join
# FANS OUT — a QBO rate that matches several VP tax codes by name yields one
# row per match, so the foreach records each as its own map row (Workato
# parity, e.g. NO TAX PURCHASE → VP codes 6,7,8,9). `ExistingVantagepointCode`
# is the matched VP code verbatim (vtc.Code); rates with no VP match take the
# create path. Idempotency comes from the (QBOCodeID, QBORateID,
# VantagepointCode) unique key on map_tax_code, not from collapsing here.
COMPILE_TAX_CODES_SQL = (
    "SELECT "
    "  tr.CodeName AS CodeName, tr.RateName AS RateName, tr.Rate AS Rate, "
    "  tr.CodeID AS CodeID, tr.RateID AS RateID, tr.TaxOn AS TaxOn, "
    "  tr.TaxTypeApplicable AS TaxTypeApplicable, tr.Active AS Active, "
    "  tcm.VantagepointCode AS MappedVantagepointCode, tcm.Rate AS MappedRate, "
    "  tcm.QBORateName AS MappedName, tcm.TaxOn AS MappedTaxOn, "
    "  vtc.Code AS ExistingVantagepointCode, "
    "  vtc.Description AS ExistingVantagepointName, "
    "  tgi.IsTaxGroup AS IsTaxGroup "
    "FROM qbo_tax_rates tr "
    "LEFT JOIN tax_code_map tcm "
    "  ON (tr.CodeName = tcm.QBOCodeName AND tr.RateName = tcm.QBORateName) "
    "   OR (tr.CodeID = tcm.QBOCodeID AND tr.RateID = tcm.QBORateID) "
    "LEFT JOIN vp_tax_codes vtc "
    "  ON tr.RateName = vtc.Description OR tcm.VantagepointCode = vtc.Code "
    "LEFT JOIN tax_group_ids tgi ON tr.CodeID = tgi.CodeID "
    "ORDER BY tr.CodeName"
)


def build_qbo_tax_rates_staging(**_context):
    """CreateCollectionOperator source for `qbo_tax_rates` (recipe step 12).

    Reads the upstream QBO TaxCode + TaxRate fetches and returns the
    flattened one-row-per-(code, rate, direction) dataset.
    """
    qbo_tax_codes = _extract_qbo_records(rail.result('fetch_qbo_tax_codes'))
    qbo_tax_rates = _extract_qbo_records(rail.result('fetch_qbo_tax_rates'))
    return flatten_qbo_tax_rates(qbo_tax_codes, qbo_tax_rates)


def prepare_vp_tax_codes_staging(**_context):
    """CreateCollectionOperator source for `vp_tax_codes` (recipe steps 10/14).

    Normalizes the `fetch_vp_tax_codes` VP GET result to a list of
    {Code, Description} rows for the `vtc` join.
    """
    result = rail.result('fetch_vp_tax_codes')
    if isinstance(result, dict):
        records = [result]
    elif isinstance(result, list):
        records = result
    else:
        records = []
    return [
        {'Code': r.get('Code'), 'Description': r.get('Description')}
        for r in records
        if isinstance(r, dict) and r.get('Code')
    ]


def read_map_tax_code_for_staging(**_context):
    """CreateCollectionOperator source for `tax_code_map` (recipe steps 11/13).

    Copies the persistent S3 `map_tax_code` rows (plus the sqlite rowid as
    `EntryID`) into a run-local collection so the Step 17 JOIN can read the
    existing mapping (`tcm`). Empty on a first sync.
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
                f'SELECT rowid, QBOCodeName, QBORateName, QBOCodeID, '
                f'VantagepointCode, Rate, TaxTypeApplicable, QBORateID, '
                f'IsTaxGroup, TaxOn FROM {MAP_TAX_CODE_TABLE_NAME}'
            )
            for (entry_id, code_name, rate_name, code_id, vp_code, rate,
                 tax_type, rate_id, is_group, tax_on) in cur.fetchall():
                rows.append({
                    'QBOCodeName': code_name,
                    'QBORateName': rate_name,
                    'QBOCodeID': code_id,
                    'VantagepointCode': vp_code,
                    'Rate': rate,
                    'TaxTypeApplicable': tax_type,
                    'QBORateID': rate_id,
                    'IsTaxGroup': is_group,
                    'TaxOn': tax_on,
                    'EntryID': entry_id,
                })
    return rows


def filter_components_needing_sync(flattened_components):
    """Filter to only components that need synchronization.

    Exactly mirrors Workato's JavaScript logic:
    if (component.MappedRate != component.Rate ||
        component.MappedRate === '' ||
        component.VantagepointCode == '' ||
        component.MappedName != component.RateName ||
        component.MappedTaxOn == '')
    """
    components_to_sync = []
    are_new_or_updated = False

    for component in flattened_components:
        # Exact match of Workato's filtering logic
        needs_sync = (
            str(component.get('MappedRate', '') or '') != str(component.get('Rate', '') or '') or
            (component.get('MappedRate', '') or '') == '' or
            (component.get('MappedVantagepointCode', '') or '') == '' or
            (component.get('MappedName', '') or '') != (component.get('RateName', '') or '') or
            (component.get('MappedTaxOn', '') or '') == ''
        )

        if needs_sync:
            are_new_or_updated = True
            components_to_sync.append(component)

    return components_to_sync, are_new_or_updated


# ===========================================================================
# TAX CODE MAPPING — body builders (QBO → VP)
# ===========================================================================
# VP /vision/TaxCodeEntity/ POST/PUT body — strict Workato parity.
# ===========================================================================
# Workato recipe `014_503_psa_sync_tax_codes` sends exactly these fields
# (POST lines 3960-3967, PUT lines 4866-4873): Description, Code, Rate,
# QBOID, QBOLastUpdated, Status. Extra fields (TaxType, IsTaxGroup,
# QBOTaxCodeID, QBOTaxRateID) cause VP to reject the request with
# 'Field <name> does not exist' — see MAP_TAX_CODE_SYNC_FIX_LOG.md #1.

def _generate_vp_tax_code():
    """Generate a new VP `Code` for a tax-rate component.

    VP's TaxCodeEntity `Code` field is short (~10 chars) and
    case-insensitive — long derived names like '<CodeName>-<RateName>'
    collide on truncation (`California-State` and
    `California-County` both become `CALIFORNIA` and VP rejects with
    'Tax Code <prefix> already exists'). Workato sidesteps this with a
    random 4-char UUID slice: `workato.uuid.to_s.upcase.slice(0,4)`
    (recipe `014_503_psa_sync_tax_codes.recipe.json` line 3884).

    We mirror that: 4 uppercase hex chars from a UUID. The generated
    code is stored in `map_tax_code.VantagepointCode` after a
    successful POST so subsequent runs route via PUT with the same
    code.

    36^4 ≈ 1.7M combinations of [0-9A-F] is hex-only ≈ 65k, but per-
    tenant the active tax-code count is tiny so collision risk is
    negligible. If a collision ever does happen, VP rejects with
    'already exists' and the per-record retry will pick a new uuid.
    """
    import uuid  # pylint: disable=import-outside-toplevel
    return uuid.uuid4().hex.upper()[:4]


def _vp_tax_code_now():
    """VP `QBOLastUpdated` timestamp — Workato uses `=now`; we mirror with
    a UTC ISO 8601 string."""
    from datetime import datetime, timezone  # pylint: disable=import-outside-toplevel
    return datetime.now(timezone.utc).isoformat()


def build_vp_tax_code_create_body(flat_row, vp_code):
    """POST /vision/TaxCodeEntity/ body for a flattened tax rate component.

    `vp_code` is the VP `Code` to assign (caller passes the value from
    `_generate_vp_tax_code` so the same value can be persisted in
    map_tax_code without a second generation).

    Workato reference: `014_503_psa_sync_tax_codes.recipe.json` POST body
    lines 3960-3967. `QBOID` carries the QBO TaxCode Id (CodeID), per
    recipe line 3964 (the foreach `Id` resolves to the QBO TaxCode's Id —
    see col3 mapping at recipe line 3294).
    """
    body = {
        'Code': vp_code,
        'Description': flat_row.get('RateName') or flat_row.get('CodeName'),
        'Rate': flat_row.get('Rate'),
        'QBOID': flat_row.get('CodeID'),
        'QBOLastUpdated': _vp_tax_code_now(),
        'Status': 'A',
    }
    return _filter_none(body)


def build_vp_tax_code_update_body(flat_row):
    """PUT /vision/TaxCodeEntity/{code} body — Code goes in the URL.

    Workato reference: `014_503_psa_sync_tax_codes.recipe.json` PUT body
    lines 4866-4873 — drops `QBOID` (immutable cross-reference) and
    includes `QBOLastUpdated`.
    Workato line 4872: Status: "=Active ? 'A' : 'I'"
    """
    body = {
        'Description': flat_row.get('RateName') or flat_row.get('CodeName'),
        'Rate': flat_row.get('Rate'),
        'QBOLastUpdated': _vp_tax_code_now(),
        # Match Workato's conditional Status based on Active flag
        'Status': 'A' if flat_row.get('Active', True) else 'I',
    }
    return _filter_none(body)


# ===========================================================================
# TAX CODE MAPPING — sync engine
# ===========================================================================

def _read_compiled_tax_codes(context):
    """Read the run-local `compiled_tax_codes` collection (recipe step 17
    output) as a list of plain dicts.

    The DAG's `query_compiled_tax_codes` task (QueryCollectionOperator)
    materializes this table in the per-DAG-run local collection; we read
    it back here to drive the foreach (recipe step 18).
    """
    import sqlite3  # pylint: disable=import-outside-toplevel
    import rail.lib.collection  # pylint: disable=import-outside-toplevel

    artifact_name = rail.lib.collection.get_collection_artifact_name(context)
    rows = []
    with rail.lib.collection.get_or_create_collection_artifact(
        artifact_name, context
    ) as artifact:
        with sqlite3.connect(artifact.local_filename) as conn:
            cur = conn.cursor()
            cur.execute(f'SELECT * FROM {COMPILED_TAX_CODES_COLLECTION}')
            columns = [d[0] for d in cur.description]
            for values in cur.fetchall():
                rows.append(dict(zip(columns, values)))
    return rows


def _build_map_tax_code_row(flat_row, vp_code):
    """Assemble one map_tax_code row dict for the batched upsert, keyed by
    (QBOCodeID, QBORateID, VantagepointCode).

    Keys cover every column the sync writes (the upsert operator builds its
    ON CONFLICT statement from the first row's keys, so all rows must share
    this exact column set). The dispatcher creates map_tax_code with a UNIQUE
    index on (QBOCodeID, QBORateID, VantagepointCode) — see
    MAP_TAX_CODE_UNIQUE_COLUMNS — so the keyed upsert keeps one row per
    *distinct VP code* per QBO rate component: the step-17 `vtc` fan-out
    (several VP codes for one rate) is preserved across rows while re-runs of
    the same (rate, VP code) converge instead of accumulating.
    """
    return {
        'QBOCodeName': flat_row.get('CodeName'),
        'QBORateName': flat_row.get('RateName'),
        'QBOCodeID': flat_row.get('CodeID'),
        'VantagepointCode': vp_code,
        'Rate': flat_row.get('Rate'),
        'TaxTypeApplicable': flat_row.get('TaxTypeApplicable'),
        'QBORateID': flat_row.get('RateID'),
        'IsTaxGroup': flat_row.get('IsTaxGroup') or 'N',
        'TaxOn': flat_row.get('TaxOn'),
    }


def _resolve_create_vp_code(rate_id, rate_name, vp_by_code):
    """VP `Code` to assign on create (Workato recipe steps 27-30).

    Default to the QBO RateId so the VP `Code` is deterministic and
    reusable across runs. Only when a VP code already exists with that
    exact `Code` but a *different* `Description` (i.e. the RateId would
    collide with an unrelated VP tax code) do we fall back to a random
    4-char UUID slice — mirroring the recipe's collision query (step 27),
    the `rows > 0 AND RateName != Description` test (step 28), and the
    `CodeUnique = workato.uuid.to_s.upcase.slice(0,4)` assignment (step 29).
    """
    existing = vp_by_code.get(str(rate_id))
    if existing and (existing.get('Description') or '') != (rate_name or ''):
        return _generate_vp_tax_code()
    return str(rate_id)


def sync_qbo_tax_codes_to_vp(instance):  # pylint: disable=unused-argument,too-many-locals,too-many-branches,too-many-statements
    """Forward sync (QBO TaxCode + TaxRate → VP Tax Codes).

    Full parity with the Workato recipe `014-503 PSA Sync Tax Codes`
    forward (initial-sync) path. This is recipe step 18 (the `foreach`);
    steps 10-17 are performed upstream by the DAG's collection operators,
    which materialize the `compiled_tax_codes` table read here.

    1. Read `compiled_tax_codes` (recipe step 17 compile JOIN output) from
       the run-local collection.
    2. Build the VP-code collision index from `fetch_vp_tax_codes`
       (recipe step 27 lookup).
    3. Filter to components that need sync (recipe step 4 change-detection).
    4. For each (Active) compiled row — and a rate that name-matches several
       VP tax codes produces several rows (Workato `vtc` fan-out) — three
       independent Workato branches:
       - #22 existing VP code + not yet mapped → adopt that code in the
         map table.
       - #24 not in VP + not mapped → POST a new VP code (Code = RateId,
         UUID on collision). Tax *groups* are only created when
         CFG_Region == 'US' (recipe step 26); otherwise the create is
         skipped but the map row is still recorded (recipe step 33).
       - #34 existing VP code + values changed → PUT the QBO values, but
         never PUT a tax group (recipe step 37).
    5. Write map_tax_code via idempotent INSERT OR REPLACE keyed on
       (QBOCodeID, QBORateID, VantagepointCode) — one row per distinct VP
       code per QBO rate component, so the fan-out rows coexist and re-runs
       of the same (rate, VP code) converge.

    Deactivation of inactive codes (recipe steps 44-49) is the
    single-record realtime path and is intentionally out of scope here.

    Reads:
      - run-local `compiled_tax_codes` (DAG query_compiled_tax_codes output)
      - rail.result('fetch_vp_tax_codes') — VP TaxCodeEntity GET (step 10)
    """
    from rail import (  # pylint: disable=import-outside-toplevel
        S3UpsertCollectionOperator,
        VantagepointTaxCodesOperator,
    )

    context = rail.get_current_context()
    log = context['task_instance'].log

    conn_ids = IntegrationConfig.get_conn_ids(context)
    vp_conn_id = conn_ids['vp_conn_id']

    # Region gate for tax groups (recipe account property 014_503_PSA.CFG_Region).
    region = (IntegrationConfig.get_cfg(context, 'CFG_Region') or '').upper()

    # VP catalog (step 10 result) indexed by Code for the create-time
    # collision check (recipe step 27). The reuse-by-name adoption itself
    # is already resolved into ExistingVantagepointCode by the step 17 JOIN.
    vp_records = rail.result('fetch_vp_tax_codes')
    if isinstance(vp_records, dict):
        vp_records = [vp_records]
    vp_by_code = {
        str(r.get('Code')): r
        for r in (vp_records or [])
        if isinstance(r, dict) and r.get('Code')
    }

    # Compiled working set (recipe step 17 output).
    compiled_rows = _read_compiled_tax_codes(context)
    log.info("Read %d compiled tax-code rows (step 17 output)", len(compiled_rows))

    # Change detection (recipe step 4 / step 5-6 early stop).
    components_to_sync, are_new_or_updated = filter_components_needing_sync(
        compiled_rows
    )

    summary = {
        'created': 0, 'updated': 0, 'mapped_existing': 0, 'skipped_group': 0,
        'skipped_no_code': 0, 'skipped_unchanged': 0, 'errors': [],
    }

    if not are_new_or_updated:
        log.info("No tax components need synchronization")
        summary['skipped_unchanged'] = len(compiled_rows)
        return summary

    log.info(
        "Filtered to %d components needing sync out of %d total",
        len(components_to_sync), len(compiled_rows)
    )
    summary['skipped_unchanged'] = len(compiled_rows) - len(components_to_sync)

    s3_integration = IntegrationConfig.S3_INTEGRATION_NAME
    s3_customer = IntegrationConfig.get_s3_customer(context)
    s3_integration_type = IntegrationConfig.get_s3_integration_type(context)

    # ---- Phase 1: all VP API work, accumulate map rows in memory ----
    # The existing map_tax_code was already read upstream (lock-free) by
    # read_map_tax_code_for_staging and JOINed into compiled_tax_codes, so
    # nothing here touches S3 — the collection lock is NOT held across the VP
    # POST/PUT round-trips.
    map_rows = []

    # Process only the filtered components (matching Workato step 18)
    for flat_row in components_to_sync:
        # Skip inactive codes in the forward path (recipe step 18).
        active = flat_row.get('Active', True)
        if active in (False, 0, '0', 'false', 'False'):
            continue

        code_id = flat_row.get('CodeID')
        rate_id = flat_row.get('RateID')
        code_name = flat_row.get('CodeName') or ''
        rate_name = flat_row.get('RateName') or ''

        if not code_id or not rate_id:
            summary['errors'].append({
                'code_id': code_id,
                'rate_id': rate_id,
                'name': f'{code_name}/{rate_name}',
                'error': 'Compiled row missing CodeID or RateID',
            })
            continue

        if not code_name and not rate_name:
            log.warning(
                "Skipping tax component %s/%s: both CodeName and "
                "RateName are blank — no Description for VP",
                code_id, rate_id,
            )
            summary['skipped_no_code'] += 1
            continue

        is_group = flat_row.get('IsTaxGroup') == 'Y'
        allow_group_write = (not is_group) or (region == 'US')
        existing_vp = flat_row.get('ExistingVantagepointCode') or ''
        mapped_vp = flat_row.get('MappedVantagepointCode') or ''
        mapped_name = flat_row.get('MappedName') or ''
        mapped_rate = flat_row.get('MappedRate')
        rate_value = flat_row.get('Rate')

        # Default: keep whatever VP code the map already points at.
        vp_code_for_map = mapped_vp

        try:
            # --- Branch #24: not in VP, not yet mapped → create ---
            if not existing_vp and not mapped_vp:
                create_code = _resolve_create_vp_code(
                    rate_id, rate_name, vp_by_code,
                )
                if allow_group_write:
                    create_body = build_vp_tax_code_create_body(
                        flat_row, create_code,
                    )
                    VantagepointTaxCodesOperator(
                        task_id=f'_post_tax_{code_id}_{rate_id}',
                        vp_conn_id=vp_conn_id,
                        request_method='POST',
                        request_body=create_body,
                        pagination=False,
                    ).execute(context)
                    summary['created'] += 1
                else:
                    # Tax group, non-US region → recipe step 26 skips
                    # the VP create, but step 33 still records the
                    # (RateId-based) code in the map table.
                    log.info(
                        "Skipping VP create for tax group %s/%s "
                        "(region=%s, not US)",
                        code_id, rate_id, region or '<unset>',
                    )
                    summary['skipped_group'] += 1
                vp_code_for_map = create_code

            # --- Branch #22: matched existing VP code, not yet mapped ---
            if existing_vp and not mapped_vp:
                vp_code_for_map = existing_vp
                summary['mapped_existing'] += 1

            # --- Branch #34: matched existing VP code → update if changed ---
            if existing_vp:
                changed = (
                    mapped_name != rate_name
                    or str(rate_value) != str(mapped_rate)
                )
                if changed and not is_group:
                    update_body = build_vp_tax_code_update_body(flat_row)
                    VantagepointTaxCodesOperator(
                        task_id=f'_put_tax_{code_id}_{rate_id}',
                        vp_conn_id=vp_conn_id,
                        request_method='PUT',
                        code=existing_vp,
                        request_body=update_body,
                        pagination=False,
                    ).execute(context)
                    summary['updated'] += 1
                vp_code_for_map = existing_vp

            # One row per (QBOCodeID, QBORateID, VantagepointCode) —
            # fan-out rows coexist, same-key re-runs converge. Accumulated
            # here; written in the Phase 2 batched upsert.
            map_rows.append(_build_map_tax_code_row(flat_row, vp_code_for_map))

        except Exception as exc:  # pylint: disable=broad-exception-caught
            log.error(
                "Failed to sync tax component %s/%s (%s/%s): %s",
                code_id, rate_id, code_name, rate_name, exc,
            )
            summary['errors'].append({
                'code_id': code_id,
                'rate_id': rate_id,
                'name': f'{code_name}/{rate_name}',
                'error': str(exc),
            })

    # ---- Phase 2: single batched upsert (one S3 lock cycle) ----
    # All accumulated rows go up in ONE download/modify/upload/lock cycle via
    # the canonical S3 collection operator, keyed on
    # (QBOCodeID, QBORateID, VantagepointCode). The old shape held the
    # collection open and locked across every VP HTTP call; this confines the
    # lock to the batched write.
    if map_rows:
        S3UpsertCollectionOperator(
            task_id='_upsert_map_tax_code',
            integration=s3_integration,
            customer=s3_customer,
            integration_type=s3_integration_type,
            collection_name=MAP_TAX_CODE_TABLE_NAME,
            key_columns=MAP_TAX_CODE_UNIQUE_COLUMNS,
            rows=map_rows,
        ).execute(context)
        log.info("Upserted %d map_tax_code row(s) in one S3 cycle.",
                 len(map_rows))
    else:
        log.info("No map_tax_code rows to upsert.")

    log.info(
        "map_tax_code sync summary: created=%d, updated=%d, mapped_existing=%d, "
        "skipped_group=%d, skipped_unchanged=%d, skipped_no_code=%d, errors=%d",
        summary['created'], summary['updated'], summary['mapped_existing'],
        summary['skipped_group'], summary['skipped_unchanged'],
        summary['skipped_no_code'], len(summary['errors'])
    )
    if summary['errors']:
        raise RuntimeError(
            f"map_tax_code sync had {len(summary['errors'])} failure(s); "
            f"first: {summary['errors'][0]}"
        )
    return summary


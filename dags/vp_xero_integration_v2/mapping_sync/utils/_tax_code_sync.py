"""
Tax code mapping sync (Xero TaxRates → VP Tax Codes).

Operator-driven port of Workato `014_501_psa_sync_tax_codes` (the GL worker the
Initial-Synch wrapper calls), with the `Map Tax Codes` seeder folded in
(Option A). See reverse-engineering docs 03-sync-tax-codes.md + 06-lookup-table-seeding.md.

Defining characteristic: Xero models tax as TaxRates with nested
TaxComponents[]. `flatten_xero_tax_rates` emits one row per ACTIVE rate ×
component (fan-out — one Xero rate can map to several VP tax codes), and
COMPOUND components are linked to their base component's VP code via VP's
`CompoundOnTaxCode` in a deferred second pass.

The DAG stages three run-local collections (xero_tax_components, vp_tax_codes,
tax_code_map) and runs `COMPILE_TAX_CODES_SQL`; this module supplies the flatten
transform, the staging sources, the SQL, and the two-pass foreach engine.

Fixes vs Workato (Q9 / Q-T1 / Q-S4):
  - Step-16 join OR-precedence bug fixed with explicit parentheses.
  - Iterates per row (the seeder's `rows.first` datapill bug is not reproduced).

Public surface (re-exported via `python_callable_method.py`):
    sync_xero_tax_codes_to_vp           — compile foreach (PythonOperator)
    flatten_xero_tax_rates              — TaxRate → per-component rows
    build_xero_tax_rates_staging        — CreateCollectionOperator source
    prepare_vp_tax_codes_staging        — CreateCollectionOperator source
    read_map_tax_code_for_staging       — CreateCollectionOperator source
    COMPILE_TAX_CODES_SQL               — QueryCollectionOperator query
"""
import rail

from vp_xero_integration_v2.mapping_sync.utils._shared import (
    _extract_xero_records,
    _filter_none,
)
from vp_xero_integration_v2.common.python_callable_method import unwrap_vp_response
from vp_xero_integration_v2.common.tables import (
    MAP_TAX_CODE_TABLE_NAME,
    MAP_TAX_CODE_UNIQUE_COLUMNS,
)
from vp_xero_integration_v2.mapping_sync.config import IntegrationConfig


# ===========================================================================
# Run-local staging collections + compile SQL
# ===========================================================================

XERO_TAX_COMPONENTS_COLLECTION = 'xero_tax_components'
VP_TAX_CODES_COLLECTION = 'vp_tax_codes'
TAX_CODE_MAP_COLLECTION = 'tax_code_map'
COMPILED_TAX_CODES_COLLECTION = 'compiled_tax_codes'

XERO_TAX_COMPONENTS_STAGING_COLUMNS = [
    'RateName', 'ComponentName', 'Rate', 'IsCompound', 'TaxType', 'ReportTaxType',
]
VP_TAX_CODES_STAGING_COLUMNS = ['Code', 'Description', 'Rate']
TAX_CODE_MAP_STAGING_COLUMNS = [
    'XeroName', 'XeroCode', 'VantagepointCode', 'Rate',
    'CompoundOnCode', 'Sequence', 'EntryID',
]

# Recipe step 16 — Xero-primary compile JOIN. The compound `RateName#ComponentName`
# subquery picks the rate's non-compound component as the base for any compound
# component. The vtc join's OR is explicitly PARENTHESIZED to fix the Workato
# precedence bug (Q-T1): `(name+code match) OR (mapped-code match)`.
COMPILE_TAX_CODES_SQL = (
    "SELECT "
    "  xtc.RateName AS XeroRateName, xtc.ComponentName AS XeroComponentName, "
    "  xtc.Rate AS XeroRate, xtc.IsCompound AS XeroIsCompound, "
    "  xtc.TaxType AS XeroTaxType, xtc.ReportTaxType AS ReportTaxType, "
    "  tcm.EntryID AS MappedEntryID, "
    "  tcm.VantagepointCode AS MappedVantagepointCode, tcm.Rate AS MappedRate, "
    "  tcm.Sequence AS MappedSequence, tcm.CompoundOnCode AS MappedCompoundOnCode, "
    "  vtc.Code AS VantagepointCode, vtc.Description AS VantagepointName, "
    "  (SELECT xtcsub.RateName || '#' || xtcsub.ComponentName "
    "     FROM xero_tax_components xtcsub "
    "    WHERE xtcsub.RateName = xtc.RateName AND xtcsub.IsCompound = 'f' "
    "      AND xtcsub.ComponentName != xtc.ComponentName AND xtc.IsCompound = 't' "
    "    LIMIT 1) AS CompoundOnCode "
    "FROM xero_tax_components xtc "
    "LEFT JOIN tax_code_map tcm "
    "  ON xtc.RateName = tcm.XeroName AND xtc.ComponentName = tcm.XeroCode "
    "LEFT JOIN vp_tax_codes vtc "
    "  ON (xtc.RateName = vtc.Description AND xtc.ComponentName = vtc.Code) "
    "  OR (tcm.VantagepointCode = vtc.Code) "
    "ORDER BY xtc.RateName, xtc.ComponentName, xtc.IsCompound, tcm.Sequence"
)


# ===========================================================================
# Flatten transform (Workato `FlattenTaxRates` JS, ported to Python)
# ===========================================================================

def flatten_xero_tax_rates(tax_rates):
    """Flatten Xero TaxRates into one row per ACTIVE rate × TaxComponent.

    Returns a list of dicts {RateName, ComponentName, Rate, IsCompound ('t'/'f'),
    TaxType, ReportTaxType}. ReportTaxType defaults to 'none' when absent. Only
    ACTIVE rates are emitted (Workato parity). IsCompound is stored as the
    SQLite-friendly 't'/'f' literals the compile subquery compares against.
    """
    rows = []
    for rate in tax_rates or []:
        if not isinstance(rate, dict):
            continue
        if str(rate.get('Status') or '').strip().upper() != 'ACTIVE':
            continue
        rate_name = rate.get('Name')
        tax_type = rate.get('TaxType')
        report_tax_type = rate.get('ReportTaxType') or 'none'
        for component in rate.get('TaxComponents') or []:
            if not isinstance(component, dict):
                continue
            rows.append({
                'RateName': rate_name,
                'ComponentName': component.get('Name'),
                'Rate': component.get('Rate'),
                'IsCompound': 't' if component.get('IsCompound') else 'f',
                'TaxType': tax_type,
                'ReportTaxType': report_tax_type,
            })
    return rows


def _reverse_charge(report_tax_type, rate_name):
    """VP ReverseCharge flag (recipe step 25/34): 'Y' when the Xero ReportTaxType
    is REVERSECHARGES or the rate name mentions 'Reverse Charge', else 'N'."""
    rtt = str(report_tax_type or '').strip().upper()
    name = str(rate_name or '')
    if rtt == 'REVERSECHARGES' or 'reverse charge' in name.lower():
        return 'Y'
    return 'N'


def _generate_vp_code(sequence):
    """Generate a VP tax code `X####` from the sequence high-water-mark
    (recipe step 23: `'X' + Sequence.to_s.rjust(4, '0')`)."""
    return 'X' + str(sequence).rjust(4, '0')


def _rates_differ(xero_rate, mapped_rate):
    """True if the Xero rate differs from the mapped rate (numeric compare with a
    string fallback)."""
    try:
        return float(xero_rate or 0) != float(mapped_rate or 0)
    except (TypeError, ValueError):
        return str(xero_rate or '') != str(mapped_rate or '')


# ===========================================================================
# Staging sources
# ===========================================================================

def build_xero_tax_rates_staging(**_context):
    """CreateCollectionOperator source for `xero_tax_components` — flattened."""
    tax_rates = _extract_xero_records(rail.result('fetch_xero_tax_rates'))
    return flatten_xero_tax_rates(tax_rates)


def prepare_vp_tax_codes_staging(**_context):
    """CreateCollectionOperator source for `vp_tax_codes`."""
    records = unwrap_vp_response(rail.result('fetch_vp_tax_codes'))
    return [
        {
            'Code': r.get('Code') or '',
            'Description': r.get('Description') or '',
            'Rate': r.get('Rate'),
        }
        for r in records
        if isinstance(r, dict) and r.get('Code')
    ]


def read_map_tax_code_for_staging(**_context):
    """CreateCollectionOperator source for `tax_code_map` (existing S3 map rows +
    rowid as EntryID). Empty on a first sync."""
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
                f'SELECT rowid, XeroName, XeroCode, VantagepointCode, Rate, '
                f'CompoundOnCode, Sequence FROM {MAP_TAX_CODE_TABLE_NAME}'
            )
            for (entry_id, xero_name, xero_code, vp_code, rate,
                 compound_on, sequence) in cur.fetchall():
                rows.append({
                    'XeroName': xero_name, 'XeroCode': xero_code,
                    'VantagepointCode': vp_code, 'Rate': rate,
                    'CompoundOnCode': compound_on, 'Sequence': sequence,
                    'EntryID': entry_id,
                })
    return rows


def _read_compiled_tax_codes(context):
    """Read the run-local `compiled_tax_codes` collection (compile output)."""
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


def _build_map_row(*, xero_name, xero_code, vp_code, rate, compound_on,
                   sequence, messages):
    """Assemble one map_tax_code row dict (all 7 columns). Natural key is
    (XeroName, XeroCode) (MAP_TAX_CODE_UNIQUE_COLUMNS)."""
    return {
        'XeroName': xero_name,
        'XeroCode': xero_code,
        'VantagepointCode': vp_code,
        'Rate': rate,
        'CompoundOnCode': compound_on,
        'Sequence': sequence,
        'Messages': messages,
    }


def _max_existing_sequence(compiled_rows):
    """Highest numeric Sequence already assigned (high-water-mark) so generated
    `X####` codes never collide on re-run."""
    max_seq = 0
    for row in compiled_rows:
        raw = row.get('MappedSequence')
        try:
            value = int(str(raw).strip())
        except (TypeError, ValueError):
            continue
        max_seq = max(max_seq, value)
    return max_seq


def build_vp_tax_code_create_body(vp_code, rate_name, xero_rate, report_tax_type):
    """POST /taxCode body (recipe step 25)."""
    return _filter_none({
        'Code': vp_code,
        'Description': rate_name,
        'Rate': xero_rate,
        'ReverseCharge': _reverse_charge(report_tax_type, rate_name),
    })


def build_vp_tax_code_rate_update_body(xero_rate, rate_name, report_tax_type):
    """PUT /taxCode/{code} body for a rate change (recipe step 34)."""
    return _filter_none({
        'Rate': xero_rate,
        'ReverseCharge': _reverse_charge(report_tax_type, rate_name),
    })


def sync_xero_tax_codes_to_vp(instance):  # pylint: disable=unused-argument,too-many-locals,too-many-branches,too-many-statements
    """Forward sync (Xero TaxRates → VP Tax Codes), two-pass.

    Pass 1 (main foreach over `compiled_tax_codes`): for each flattened rate ×
    component — reuse an existing VP code (name-match or mapped), or generate an
    `X####` code and create the VP tax code; update the rate if it drifted;
    accumulate the map row and any compound link to resolve later.

    Pass 2 (compound links): for each compound component, resolve its base
    component's VP code and PUT `CompoundOnTaxCode`, recording the base code in
    the map row's CompoundOnCode (col5).

    All map rows are written in a single batched upsert keyed (XeroName, XeroCode).
    Raises RuntimeError at the end if any per-record failure occurred.
    """
    from rail import (  # pylint: disable=import-outside-toplevel
        S3UpsertCollectionOperator,
        VantagepointTaxCodesOperator,
    )

    context = rail.get_current_context()
    log = context['task_instance'].log

    conn_ids = IntegrationConfig.get_conn_ids(context)
    vp_conn_id = conn_ids['vp_conn_id']

    compiled_rows = _read_compiled_tax_codes(context)
    log.info("Read %d compiled tax-component rows", len(compiled_rows))

    s3_integration = IntegrationConfig.S3_INTEGRATION_NAME
    s3_customer = IntegrationConfig.get_s3_customer(context)
    s3_integration_type = IntegrationConfig.get_s3_integration_type(context)

    summary = {'created': 0, 'updated': 0, 'reused_existing': 0,
               'compound_linked': 0, 'errors': []}

    sequence = _max_existing_sequence(compiled_rows)
    # (XeroName, XeroCode) -> map row dict (so the compound pass can patch col5)
    map_rows_by_key = {}
    # (RateName, ComponentName) -> resolved VP code (for compound base lookup)
    component_to_vpcode = {}
    compound_links = []  # (rate_name, component_name, 'BaseRate#BaseComponent')

    # ---- Pass 1: create / reuse / rate-update (no S3 lock) ----
    for row in compiled_rows:
        rate_name = row.get('XeroRateName')
        component_name = row.get('XeroComponentName')
        if rate_name is None or component_name is None:
            continue
        xero_rate = row.get('XeroRate')
        is_compound = str(row.get('XeroIsCompound') or '').strip().lower() == 't'
        report_tax_type = row.get('ReportTaxType')
        mapped_vp_code = row.get('MappedVantagepointCode') or ''
        mapped_rate = row.get('MappedRate')
        mapped_seq = row.get('MappedSequence') or ''
        mapped_compound = row.get('MappedCompoundOnCode') or ''
        vtc_code = row.get('VantagepointCode') or ''
        compound_on_ref = row.get('CompoundOnCode') or ''

        key = (rate_name, component_name)
        messages = ''
        try:
            resolved_code = mapped_vp_code or vtc_code
            if not resolved_code:
                # Brand-new component → generate code + create VP tax code.
                sequence += 1
                resolved_code = _generate_vp_code(sequence)
                try:
                    VantagepointTaxCodesOperator(
                        task_id=f'_post_tax_code_{resolved_code}',
                        vp_conn_id=vp_conn_id,
                        request_method='POST',
                        request_body=build_vp_tax_code_create_body(
                            resolved_code, rate_name, xero_rate, report_tax_type),
                        pagination=False,
                    ).execute(context)
                    summary['created'] += 1
                    row_sequence = str(sequence)
                except Exception as create_exc:  # pylint: disable=broad-exception-caught
                    if 'already exists' in str(create_exc).lower():
                        # Code was created by a prior partial run; adopt it so
                        # the S3 mapping is written and future runs skip the POST.
                        # Always PUT the current Xero rate: we have no mapped_rate
                        # baseline to compare against, so we can't skip the update.
                        log.warning("Tax code %s (%s/%s) already exists in VP — adopting "
                                    "and aligning rate to %.4f",
                                    resolved_code, rate_name, component_name,
                                    float(xero_rate or 0))
                        try:
                            VantagepointTaxCodesOperator(
                                task_id=f'_put_tax_code_{resolved_code}_adopt',
                                vp_conn_id=vp_conn_id,
                                request_method='PUT',
                                code=resolved_code,
                                request_body=build_vp_tax_code_rate_update_body(
                                    xero_rate, rate_name, report_tax_type),
                                pagination=False,
                            ).execute(context)
                        except Exception as put_exc:  # pylint: disable=broad-exception-caught
                            log.warning("Tax code %s rate-align PUT failed (non-fatal): %s",
                                        resolved_code, put_exc)
                        summary['reused_existing'] += 1
                    else:
                        log.error("Tax code %s (%s/%s) create failed: %s",
                                  resolved_code, rate_name, component_name, create_exc)
                        summary['errors'].append({
                            'rate': rate_name, 'component': component_name,
                            'error': str(create_exc),
                        })
                        continue
                    row_sequence = str(sequence)
            else:
                # Existing VP code (mapped or name-matched) → rate-update if drifted.
                row_sequence = str(mapped_seq) if mapped_seq else ''
                if _rates_differ(xero_rate, mapped_rate):
                    VantagepointTaxCodesOperator(
                        task_id=f'_put_tax_code_{resolved_code}',
                        vp_conn_id=vp_conn_id,
                        request_method='PUT',
                        code=resolved_code,
                        request_body=build_vp_tax_code_rate_update_body(
                            xero_rate, rate_name, report_tax_type),
                        pagination=False,
                    ).execute(context)
                    summary['updated'] += 1
                else:
                    summary['reused_existing'] += 1

            component_to_vpcode[key] = resolved_code
            map_rows_by_key[key] = _build_map_row(
                xero_name=rate_name, xero_code=component_name,
                vp_code=resolved_code, rate=xero_rate,
                compound_on=mapped_compound, sequence=row_sequence,
                messages=messages,
            )

            if is_compound and compound_on_ref and not mapped_compound:
                compound_links.append((rate_name, component_name, compound_on_ref))

        except Exception as exc:  # pylint: disable=broad-exception-caught
            log.error("Failed to sync Xero tax %s/%s: %s",
                      rate_name, component_name, exc)
            summary['errors'].append({
                'rate': rate_name, 'component': component_name, 'error': str(exc),
            })

    # ---- Pass 2: compound links (base must exist first; ordered by IsCompound) ----
    for rate_name, component_name, compound_on_ref in compound_links:
        try:
            own_code = component_to_vpcode.get((rate_name, component_name))
            base_parts = compound_on_ref.split('#', 1)
            base_key = (base_parts[0], base_parts[1] if len(base_parts) > 1 else '')
            base_code = component_to_vpcode.get(base_key)
            if not own_code or not base_code:
                log.warning(
                    "Compound link skipped for %s/%s: own=%r base=%r (ref %r)",
                    rate_name, component_name, own_code, base_code, compound_on_ref)
                continue
            VantagepointTaxCodesOperator(
                task_id=f'_put_compound_{own_code}',
                vp_conn_id=vp_conn_id,
                request_method='PUT',
                code=own_code,
                request_body={'CompoundOnTaxCode': base_code},
                pagination=False,
            ).execute(context)
            summary['compound_linked'] += 1
            map_row = map_rows_by_key.get((rate_name, component_name))
            if map_row is not None:
                map_row['CompoundOnCode'] = base_code
        except Exception as exc:  # pylint: disable=broad-exception-caught
            log.error("Failed to link compound tax %s/%s: %s",
                      rate_name, component_name, exc)
            summary['errors'].append({
                'rate': rate_name, 'component': component_name,
                'error': f'compound link failed: {exc}',
            })

    # ---- Pass 3: single batched upsert keyed (XeroName, XeroCode) ----
    map_rows = list(map_rows_by_key.values())
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
        log.info("Upserted %d map_tax_code row(s).", len(map_rows))
    else:
        log.info("No map_tax_code rows to upsert.")

    log.info("map_tax_code sync summary: %s", summary)
    if summary['errors']:
        raise RuntimeError(
            f"map_tax_code sync had {len(summary['errors'])} failure(s); "
            f"first: {summary['errors'][0]}"
        )
    return summary

"""
Firm mapping sync (sections H[partial]+I+K from the pre-split file).

QBO Customer/Vendor → VP Firm mapping. The forward sync is map-only
(Workato parity): entities that already resolve to a VP firm are updated
(PUT) + VendorAccountingInfo upserted for vendors; entities with no VP
firm are recorded UNMAPPED (blank FirmID) and NOT created. See section
banner comments below and MAP_FIRM_SYNC_FIX_LOG.md #1-#12 for the
per-row fix history.

Public surface (re-exported via `python_callable_method.py`):
    sync_qbo_firms_to_vp
"""
import logging

import rail
from airflow.models import Variable

# Shared helpers still live in `python_callable_method.py` during the
# staged split: `_resolve_cfg_then_variable` (used by
# `lookup_default_vendor_type` below), and the QBO/VP response
# normalisers used by the sync engine. They move to `_shared.py` in
# the final extraction step.
from vp_quickbooks_integration.mapping_sync.utils._shared import (
    _filter_none,
    _resolve_cfg_then_variable,
    _extract_qbo_records,
    _extract_qbo_entity_id,
)
from vp_quickbooks_integration.common.tables import (
    MAP_FIRM_TABLE_NAME,
    MAP_FIRM_UNIQUE_COLUMNS,
)
from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig

_log = logging.getLogger(__name__)


def lookup_default_vendor_type(instance):
    """Default Category for new VP vendors (Workato CFG_DefaultVendorType).

    Resolution: `dag_run.conf['config']['CFG_DefaultVendorType']` →
    Airflow Variable `vp_qbo_mapping_sync_default_vendor_type_{instance}`
    → None. The firm body builder drops the Category field via
    `_filter_none` when this returns None.
    """
    return _resolve_cfg_then_variable(
        'CFG_DefaultVendorType',
        f'vp_qbo_mapping_sync_default_vendor_type_{instance}',
    )



# ===========================================================================
# FIRM MAPPING — body builders (QBO Customer/Vendor → VP Firm)
# Recipes:
#   014_503_psa_synch_firms.recipe.json
#   014_503_psa_quickbooks_customer_vendor_to_vantagepoint.recipe.json
#   014_503_psa_vantagepoint_upsert_firm_address.recipe.json
# Field shapes mirror vendor_sync's build_create_firm_body for parity.
# ===========================================================================

def _qbo_status_to_vp_status(active):
    """QBO `Active` (bool) → VP `Status` ('A'/'I')."""
    if active is True:
        return 'A'
    if active is False:
        return 'I'
    return None


def build_vp_firm_create_body_from_qbo(qbo_record, is_vendor, instance):
    """POST /firm body. Direction: QBO Customer or Vendor → VP Firm.

    NOTE: As of the Issue-B (Workato-parity) change, the forward sync
    `sync_qbo_firms_to_vp` no longer creates VP firms — QBO entities with
    no existing VP firm are left unmapped. This builder (and
    `build_vp_firm_address_body_from_qbo` /
    `build_vp_firm_contact_body_from_qbo`) is retained for a potential
    explicit create/reverse flow and to preserve the hard-won field-shape
    knowledge documented in MAP_FIRM_SYNC_FIX_LOG.md; it is intentionally
    not invoked by the current forward sync.

    `is_vendor=True` produces a vendor-style body (VendorInd='Y',
    ClientInd='N', sets `Vendor`=QBOID and `Category` from per-instance
    Variable). `is_vendor=False` produces a client-style body
    (VendorInd='N', ClientInd='Y').

    Org / Type / Client are intentionally omitted — empty strings caused
    "Please provide a Relationship for table Firm" rejections in tenant
    testing (see vendor_sync notes). VendorInd / ClientInd establish the
    relationship instead.
    """
    qbo_id = qbo_record.get('Id')
    web_addr = qbo_record.get('WebAddr') or {}
    primary_email = qbo_record.get('PrimaryEmailAddr') or {}
    qb_company = qbo_record.get('CompanyName') or qbo_record.get('DisplayName')

    body = {
        'QBOID': qbo_id,
        'Name': qb_company,
        'SortName': qb_company,
        'Status': _qbo_status_to_vp_status(qbo_record.get('Active')),
        'WebSite': web_addr.get('URI'),
        'PrimaryEmail': primary_email.get('Address'),
        'VendorInd': 'Y' if is_vendor else 'N',
        'ClientInd': 'N' if is_vendor else 'Y',
        'ExportInd': False,
        'PriorWork': False,
        'Recommend': False,
        'GovernmentAgency': False,
        'Competitor': False,
        'AvailableForCRM': 'N',
        'ReadyForApproval': False,
        'ReadyForProcessing': 'N',
    }
    if is_vendor:
        # Vendor field = QBOID (downstream VEAccounting lookup is trivial)
        body['Vendor'] = qbo_id
        body['Category'] = lookup_default_vendor_type(instance)

    return _filter_none(body)


def build_vp_firm_update_body_from_qbo(qbo_record, is_vendor):
    """PUT /firm/{ClientID} body. Direction: QBO Customer or Vendor → VP Firm.

    Mirrors vendor_sync's build_update_firm_body: Category is intentionally
    NOT updated on PUT (Category=skip in the recipe).
    """
    qbo_id = qbo_record.get('Id')
    web_addr = qbo_record.get('WebAddr') or {}
    primary_email = qbo_record.get('PrimaryEmailAddr') or {}
    qb_company = qbo_record.get('CompanyName') or qbo_record.get('DisplayName')

    body = {
        'QBOID': qbo_id,
        'Name': qb_company,
        'SortName': qb_company,
        'Status': _qbo_status_to_vp_status(qbo_record.get('Active')),
        'WebSite': web_addr.get('URI'),
        'PrimaryEmail': primary_email.get('Address'),
        'VendorInd': 'Y' if is_vendor else 'N',
        'ClientInd': 'N' if is_vendor else 'Y',
    }
    if is_vendor:
        body['Vendor'] = qbo_id

    return _filter_none(body)


# Map QBO country aliases to VP's canonical country list. VP rejects
# anything not in its master list with "Country Code <X> does not exist"
# at /firm POST/PUT time, so common QBO short forms (USA, US, etc.) need
# to be normalized before the body goes out. Extend this map as new QBO
# tenants surface additional aliases.
_VP_COUNTRY_ALIASES = {
    'us': 'United States',
    'usa': 'United States',
    'u.s.': 'United States',
    'u.s.a.': 'United States',
    'united states of america': 'United States',
    'america': 'United States',
    'uk': 'United Kingdom',
    'u.k.': 'United Kingdom',
    'great britain': 'United Kingdom',
    'ca': 'Canada',
}


def _normalize_vp_country(qbo_country):
    """Translate a QBO BillAddr.Country value into something VP's country
    list accepts. Returns None for falsy input. Unknown values pass
    through unchanged (and may still fail at VP — extend
    ``_VP_COUNTRY_ALIASES`` when that happens).
    """
    if not qbo_country:
        return None
    stripped = qbo_country.strip()
    return _VP_COUNTRY_ALIASES.get(stripped.lower(), stripped)


def build_vp_firm_address_body_from_qbo(qbo_record):
    """POST /firm/{ClientID}/address body. Maps QBO BillAddr → VP firm-address.

    VP firm-address uses 'true'/'false' literals for PrimaryInd/Payment/Billing
    (unlike the firm record itself which uses 'Y'/'N' for VendorInd/ClientInd).
    Returns None if no address fields are present.
    """
    bill_addr = qbo_record.get('BillAddr') or {}
    if not any(bill_addr.get(f) for f in ('Line1', 'Line2', 'Line3', 'City')):
        return None

    primary_phone = qbo_record.get('PrimaryPhone') or {}

    body = {
        'PrimaryInd': 'true',
        'Payment': 'true',
        'Billing': 'true',
        'Address1': bill_addr.get('Line1'),
        'Address2': bill_addr.get('Line2'),
        'Address3': bill_addr.get('Line3'),
        'City': bill_addr.get('City'),
        'State': bill_addr.get('CountrySubDivisionCode'),
        'Zip': bill_addr.get('PostalCode'),
        'Country': _normalize_vp_country(bill_addr.get('Country')),
        'Phone': primary_phone.get('FreeFormNumber'),
    }
    return _filter_none(body)


def build_vp_firm_contact_body_from_qbo(qbo_record, client_id):
    """POST /contact body. Returns None when no contact name is present.

    Schema mirrors vendor_sync.build_create_contact_body (the canonical
    contact body in this repo):

      - VP names the firm reference ``ClientID`` (not ``FirmID``; the
        latter is rejected with "Field FirmID does not exist").
      - VP's contact email is ``Email`` (not ``PrimaryEmail``).
      - VP's mobile field is ``CellPhone`` (not ``Mobile``).
      - VP requires ``ContactStatus`` ('A' for active).
      - Primary-contact flag is ``QBOIsMainContact='true'`` (not
        ``IsPrimary``).
      - ``QBOID`` is set so the contact is traceable back to QBO.
    """
    given = (qbo_record.get('GivenName') or '').strip()
    family = (qbo_record.get('FamilyName') or '').strip()
    # VP enforces LastName at the API level ("Please provide a Last for
    # table Contacts") even when FirstName is populated. Skip the
    # contact entirely when FamilyName is missing — fabricating a
    # placeholder LastName would leak into VP's contact list and
    # corrupt reporting.
    if not family:
        return None

    primary_email = qbo_record.get('PrimaryEmailAddr') or {}
    primary_phone = qbo_record.get('PrimaryPhone') or {}
    mobile = qbo_record.get('Mobile') or {}
    fax = qbo_record.get('Fax') or {}

    body = {
        'ClientID': client_id,
        'ContactStatus': 'A',
        'FirstName': given,
        'LastName': family,
        'Email': primary_email.get('Address'),
        'Phone': primary_phone.get('FreeFormNumber'),
        'CellPhone': mobile.get('FreeFormNumber'),
        'Fax': fax.get('FreeFormNumber'),
        'QBOID': qbo_record.get('Id'),
        'QBOIsMainContact': 'true',
    }
    return _filter_none(body)


# Same default `vendor_sync` uses (vendor_sync/config.py:default_pay_terms).
# VP's pay-terms field is a foreign-key reference; an empty string makes
# the server look up a pay-terms record with code '', find nothing,
# dereference a null, and raise "Object reference not set to an instance
# of an object". Always send a non-empty value — either a mapped lookup
# or this hardcoded fallback that vendor_sync uses against the same
# tenant.
_DEFAULT_PAY_TERMS = 'Next'


def _resolve_vp_firm_for_veaccounting(client_id, vp_conn_id, context):
    """Read the two pieces of state needed to drive the
    VendorAccountingInfo upsert:

      - ``vendor_code``: the VP-stored ``Vendor`` value. VP's tenant
        numbering rule rewrites whatever we wrote on the firm POST/PUT
        (e.g. ``56`` -> ``000056``), so we read back what VP actually
        stored rather than trust the value we sent. Source:
        ``GET /api/firm/{ClientID}``. Mirrors
        `vendor_sync._resolve_vp_vendor_code` ("we don't ASSUME the
        stored value matches").
      - ``existing_ve_accounting``: the firm's current VendorAccountingInfo
        rows. **Not** read from the firm root response (that field is
        often absent or empty even when records exist); the recipe
        hits the dedicated sub-resource
        ``GET /api/firm/{ClientID}/vendorAccountingInfo`` for this
        check. ``vendor_sync.vendor_update_dag`` does the same. The
        recipe's UPSERT branches on whether this returns rows:
        non-empty => PUT update via the firm root, empty => POST
        insert via /vision/firm/VendorAccountingInfo.

    Two round-trips per vendor. Could be parallelised; left
    sequential for clarity.

    Returns ``(vendor_code, existing_ve_accounting, vendor_type)``. Any
    component may be ``None`` / ``[]`` if VP returned nothing.
    """
    from rail import (  # pylint: disable=import-outside-toplevel
        VantagepointFirmOperator,
        VantagepointAPIOperator,
    )

    # 1. Firm root -> Vendor code.
    firm_result = VantagepointFirmOperator(
        task_id=f'_get_firm_for_veaccounting_{client_id}',
        vp_conn_id=vp_conn_id,
        request_method='GET',
        client_id=client_id,
        pagination=False,
    ).execute(context)

    if isinstance(firm_result, list) and firm_result:
        firm = firm_result[0] if isinstance(firm_result[0], dict) else {}
    elif isinstance(firm_result, dict):
        firm = firm_result
    else:
        firm = {}

    vendor_code = firm.get('Vendor')
    vendor_code = str(vendor_code) if vendor_code not in (None, '') else None
    # VP's `Category` field is the UI's "Vendor Type". When this is unset
    # on an existing vendor firm (seeded outside the integration, or
    # created by a prior partial run), every PUT to /api/firm/{ClientID}
    # re-validates required firm-level fields and fails with "Please
    # provide a Vendor Type for table Firm" — including PUTs that only
    # touch the VEAccounting sub-resource. Surface it so the caller can
    # skip the upsert cleanly instead of letting the PUT fail downstream.
    vendor_type = firm.get('Category') or None

    # 2. Dedicated VendorAccountingInfo sub-resource -> existence check.
    #    Some tenants surface an empty list, some return a 404 / empty
    #    dict, some wrap a single row as a dict. Normalize all of those
    #    to a list of non-empty dicts so the caller can `if existing:`
    #    cleanly. Any exception (e.g. 404) is treated as "no existing
    #    rows" rather than failing the whole vendor sync.
    try:
        veacct_result = VantagepointAPIOperator(
            task_id=f'_get_veaccounting_{client_id}',
            vp_conn_id=vp_conn_id,
            endpoint=f'/firm/{client_id}/vendorAccountingInfo',
            request_method='GET',
            pagination=False,
        ).execute(context)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # Surface the failure rather than swallow it silently. If the
        # sub-resource GET starts failing — bad endpoint, auth issue,
        # VP outage — we want to see it in the log instead of falling
        # through to the insert branch and NPE'ing on a duplicate.
        # The caller still gets `[]` so behaviour matches "no existing
        # rows", but the warning makes the diagnostic obvious.
        import logging  # pylint: disable=import-outside-toplevel
        logging.getLogger(__name__).warning(
            "GET /api/firm/%s/vendorAccountingInfo failed: %s — "
            "treating as 'no existing rows' (POST insert branch). "
            "If the vendor actually has rows in VP this will surface "
            "as an NPE on the POST.",
            client_id, exc,
        )
        veacct_result = None

    if isinstance(veacct_result, list):
        existing = veacct_result
    elif isinstance(veacct_result, dict):
        existing = [veacct_result]
    else:
        existing = []
    existing = [row for row in existing if isinstance(row, dict) and row]

    return vendor_code, existing, vendor_type


def build_ve_accounting_update_body(qbo_record, existing_ve_accounting):
    """PUT /api/firm/{ClientID} body for the update branch of the
    VendorAccountingInfo upsert.

    Mirrors Workato recipe ``014_503_psa_dvp_insert_update_veaccounting``
    PUT step + ``vendor_sync.build_update_veaccounting_body``. Recipe
    intentionally sends only ``PayTerms`` and ``Req1099`` on the update
    path — the rest of the VendorAccountingInfo fields (DiscPct,
    EFTAddenda, etc.) stay at whatever VP already has on the existing
    row.

    ``PayTerms`` resolution order (matches the recipe):

      1. Lookup ``vp_qbo_vendor_sync_pay_terms_map`` Variable using the
         QBO ``TermRef.value`` (or ``SalesTermRef.value`` fallback).
      2. If unmapped, reuse the existing row's ``PayTerms`` so the
         update is a no-op for that field.
      3. If still empty, fall back to ``''`` — the caller's existing
         data is preserved if VP treats empty string as "leave alone"
         on this field (some endpoints do; the Workato recipe gets
         away with this because step 2 almost always populates it).
    """
    from airflow.models import Variable  # pylint: disable=import-outside-toplevel

    term_ref = (
        qbo_record.get('TermRef')
        or qbo_record.get('SalesTermRef')
        or {}
    )
    pay_terms_map = Variable.get(
        'vp_qbo_vendor_sync_pay_terms_map',
        default_var={},
        deserialize_json=True,
    )
    if not isinstance(pay_terms_map, dict):
        pay_terms_map = {}
    pay_terms = pay_terms_map.get(str(term_ref.get('value') or ''), '')

    if not pay_terms:
        for row in existing_ve_accounting:
            if row.get('PayTerms'):
                pay_terms = row['PayTerms']
                break

    # Final fallback — same `_DEFAULT_PAY_TERMS` constant the create
    # path uses, matching vendor_sync.build_update_veaccounting_body's
    # behaviour. Empty PayTerms on PUT would still cause VP to
    # dereference null on the related pay-terms record.
    if not pay_terms:
        pay_terms = _DEFAULT_PAY_TERMS

    vendor_1099 = qbo_record.get('Vendor1099')
    if vendor_1099 is True:
        req_1099 = 'Y'
    elif vendor_1099 is False:
        req_1099 = 'N'
    else:
        req_1099 = 'N'

    return {
        'VEAccounting': [{
            'PayTerms': pay_terms,
            'Req1099': req_1099,
        }]
    }


def build_ve_accounting_body(qbo_record, vp_vendor_code):
    """POST /vision/firm/VendorAccountingInfo body (vendors only).

    Mirrors Workato recipe
    ``014_503_psa_dvp_insert_update_veaccounting`` and
    ``vendor_sync.build_create_veaccounting_body``.

    History: the previous one-line ``{'ClientID': client_id}`` body
    surfaced as a C# ``Object reference not set to an instance of an
    object`` from VP — the endpoint dereferences ``Vendor`` (the VP
    vendor code, NOT the firm ClientID) plus a handful of paired
    fields the Workato recipe always sends as defaults. A second
    iteration sent ``Vendor: str(qbo_record['Id'])`` (the raw QBO Id)
    and still NPEd, because VP's tenant numbering rule rewrites the
    stored Vendor code (e.g. ``56`` -> ``000056``) and the lookup by
    raw QBO id no longer matches. ``vp_vendor_code`` here is the
    VP-stored value resolved at call time via
    :func:`_resolve_vp_vendor_code`.

    Field map:

      - ``Vendor``: VP-resolved vendor code (passed in by caller).
      - ``PayTerms`` / ``PayTermsDesc``: looked up from QBO
        TermRef.value (or SalesTermRef.value for customer-firms that
        somehow flow through here) via the shared
        ``vp_qbo_vendor_sync_pay_terms_map`` Variable, falling back
        to an empty string when no mapping exists.
      - ``Req1099``: 'Y' / 'N' from the QBO ``Vendor1099`` flag,
        defaulting to 'N' when absent.
      - Discount + 1099-running-total fields default to ``0``.
      - Check-per-voucher and EFT flags default to 'N' per recipe.
      - ``Company`` and ``ElectronicPaymentMethodID`` are
        intentionally OMITTED, not sent as empty strings. The
        Workato recipe uses ``=blank`` which is shorthand meaning
        "don't send this field" — sending it as ``''`` to VP fails
        with "Cannot insert NULL into Company column" (see the
        ``build_create_veaccounting_body`` docstring in
        vendor_sync for the historical context).
    """
    from airflow.models import Variable  # pylint: disable=import-outside-toplevel

    term_ref = (
        qbo_record.get('TermRef')
        or qbo_record.get('SalesTermRef')
        or {}
    )
    pay_terms_map = Variable.get(
        'vp_qbo_vendor_sync_pay_terms_map',
        default_var={},
        deserialize_json=True,
    )
    if not isinstance(pay_terms_map, dict):
        pay_terms_map = {}
    pay_terms = pay_terms_map.get(str(term_ref.get('value') or ''), '')

    # PayTerms must never be empty — VP's pay-terms field is a
    # foreign-key reference; '' makes the server NPE on lookup. Match
    # vendor_sync.build_create_veaccounting_body which always falls
    # back to `default_pay_terms` ('Next').
    if not pay_terms:
        pay_terms = _DEFAULT_PAY_TERMS

    vendor_1099 = qbo_record.get('Vendor1099')
    if vendor_1099 is True:
        req_1099 = 'Y'
    elif vendor_1099 is False:
        req_1099 = 'N'
    else:
        req_1099 = 'N'

    # Field shape must match vendor_sync.build_create_veaccounting_body
    # exactly. Notable invariants relearned the hard way:
    #   - `ElectronicPaymentMethodID` is SENT as '' (not omitted).
    #     vendor_sync includes it; omitting it was probably a
    #     contributor to the persistent NPE.
    #   - `Company` is NOT sent (omitted). vendor_sync's docstring:
    #     "Empty string was rejected by VP ('Cannot insert NULL into
    #     Company column'). Omitting lets VP's server-side default
    #     apply."
    return {
        'Vendor': str(vp_vendor_code or ''),
        'PayTerms': pay_terms,
        'PayTermsDesc': pay_terms,
        'Req1099': req_1099,
        'DiscPct': 0,
        'DiscPeriod': 0,
        'ThisYear1099': 0,
        'LastYear1099': 0,
        'CheckPerVoucher': 'N',
        'MemoPrintOnCheck': 'N',
        'EFTAddenda': 'N',
        'EFTRemittance': 'N',
        'EFTClieOp': 'N',
        'ElectronicPaymentMethodID': '',
    }



def _load_vp_firms_by_qboid(vp_conn_id, context):
    """Bulk-load every VP firm and index by (QBOID, VendorInd).

    Called once at the top of `sync_qbo_firms_to_vp`. Replaces what was
    a per-record `_find_vp_firm_by_qbo_id` GET — for a tenant with N
    QBO customers + vendors the old shape issued N sequential VP API
    calls; this one paginated call serves all N in-memory lookups.

    Why we need it at all: map_firm starts empty on first run, but
    the VP tenant may already have firms with QBOID populated (from
    prior Workato runs, manual imports, etc.). If we POST /firm
    blindly, VP auto-assigns a Firm Number / Vendor Number that
    collides with the existing record and the create fails with
    "Firm Number already exists" / "Vendor Number already exists".
    Pre-resolving lets us route those records to the PUT update path
    with the existing ClientID. Once a record's map_firm row is
    populated, subsequent runs skip this index entirely via the
    local cache.

    Index key shape: `(str(QBOID), VendorInd)` where `VendorInd` is
    `'Y'`/`'N'` (defaults to `'N'` per VP's customer-default behaviour).
    This belt-and-suspenders pairing handles tenants that somehow
    have both a customer and a vendor with the same QBOID — they
    don't collide.

    VP records with no QBOID are skipped at index time (they can't
    contribute to QBOID-keyed lookups; including them would just
    bloat the dict).

    Returns: `dict[(qboid_str, vendor_ind), firm_dict]`.
    """
    from rail import VantagepointFirmOperator  # pylint: disable=import-outside-toplevel

    result = VantagepointFirmOperator(
        task_id='_bulk_get_vp_firms_for_qboid_index',
        vp_conn_id=vp_conn_id,
        request_method='GET',
        pagination=True,
    ).execute(context)

    if isinstance(result, dict):
        records = [result]
    elif isinstance(result, list):
        records = [r for r in result if isinstance(r, dict)]
    else:
        return {}

    index = {}
    for record in records:
        qboid = record.get('QBOID')
        if not qboid:
            continue
        vendor_ind = record.get('VendorInd') or 'N'
        index[(str(qboid), vendor_ind)] = record
    return index


def _load_existing_map_firm_index(cur):
    """Read map_firm rows from the open sqlite cursor, indexed by (QBOID, IsVendor).

    Empty `Is Vendor` values are normalized to 'N' so callers can match
    deterministically on a customer (which uses 'N').
    """
    cur.execute(
        f'SELECT FirmID, QBOID, IsVendor, Name FROM {MAP_FIRM_TABLE_NAME}'
    )
    index = {}
    for firm_id, qbo_id, is_vendor, name in cur.fetchall():
        if qbo_id:
            key = (str(qbo_id), (is_vendor or 'N'))
            index[key] = {
                'FirmID': firm_id,
                'QBOID': qbo_id,
                'IsVendor': is_vendor,
                'Name': name,
            }
    return index


def _build_map_firm_row(firm_id, qbo_id, is_vendor_flag, name):
    """Assemble one map_firm row dict for the batched upsert.

    Keys cover every column in MAP_FIRM_COLUMNS (the upsert operator builds
    its ON CONFLICT statement from the first row's keys, so all rows must
    share this exact column set — mapped rows carry a resolved FirmID,
    unmapped rows carry a blank FirmID). The natural key is
    (QBOID, IsVendor) — see MAP_FIRM_UNIQUE_COLUMNS, declared as a UNIQUE
    index by `dispatcher_dag.init_mapping_collections` — so a re-sync of the
    same QBO entity replaces its row in place (FirmID / Name refreshed via
    ON CONFLICT DO UPDATE) instead of stacking a duplicate, while a customer
    and a vendor sharing a QBOID stay distinct rows.
    """
    return {
        'FirmID': firm_id,
        'QBOID': qbo_id,
        'IsVendor': is_vendor_flag,
        'Name': name,
    }


def sync_qbo_firms_to_vp(instance):
    """For each fetched QBO Customer / Vendor, map it to VP and upsert the
    map_firm cross-reference row.

    Workato parity (forward sync is map-only, NOT create):
      - If the QBO entity already resolves to a VP firm (via a populated
        local map_firm row, or the bulk VP-by-QBOID index), the firm is
        updated (PUT) — plus VendorAccountingInfo upsert for vendors —
        and the row is written with that VP ClientID as FirmID.
      - Otherwise the QBO entity is left UNMAPPED: the row is written with
        a blank FirmID and no VP firm is created. This mirrors Workato's
        `014_503_psa_synch_firms`, whose lookup table legitimately holds
        QBO-native entities with an empty ClientID.

    Called by map_firm_dag's `process_qbo_firms` PythonOperator.

    Reads:
      - rail.result('fetch_qbo_customers') — list of QBO Customer records
      - rail.result('fetch_qbo_vendors')   — list of QBO Vendor records

    Returns a summary dict {updated, unmapped, backfilled_vendor_type,
    errors} (errors is a list of {qbo_id, name, error} dicts). The
    PythonOperator that wraps this is marked with the standard
    catch_*_dag_error rule.
    """
    import sqlite3  # pylint: disable=import-outside-toplevel
    import rail.lib.s3_collection  # pylint: disable=import-outside-toplevel
    from rail import (  # pylint: disable=import-outside-toplevel
        S3UpsertCollectionOperator,
        VantagepointFirmOperator,
        VantagepointCustomOperator,
    )

    context = rail.get_current_context()
    log = context['task_instance'].log

    customers = _extract_qbo_records(rail.result('fetch_qbo_customers'))
    vendors = _extract_qbo_records(rail.result('fetch_qbo_vendors'))
    log.info(
        "Processing QBO firms: %d customers, %d vendors → VP",
        len(customers), len(vendors),
    )

    conn_ids = IntegrationConfig.get_conn_ids(context)
    vp_conn_id = conn_ids['vp_conn_id']

    s3_integration = IntegrationConfig.S3_INTEGRATION_NAME
    s3_customer = IntegrationConfig.get_s3_customer(context)
    s3_integration_type = IntegrationConfig.get_s3_integration_type(context)
    s3_artifact_name = rail.lib.s3_collection.get_s3_collection_artifact_name(
        context, s3_integration, s3_customer, s3_integration_type
    )

    # 'updated'  — QBO entity resolved to an existing VP firm (PUT).
    # 'unmapped' — no VP firm; map_firm row written with a blank FirmID
    #              (Workato parity — the forward sync never creates VP
    #              firms). No 'created' counter: creation was removed.
    summary = {'updated': 0, 'unmapped': 0,
               'backfilled_vendor_type': 0, 'errors': []}

    # ---- Phase 0: load existing map_firm index (no S3 lock) ----
    # Read-only snapshot used purely for mapped-vs-unmapped discovery. Opened
    # with use_lock=False so the HTTP work in Phase 1 never holds the S3
    # collection lock (mirrors read_account_code_map_for_staging). The read is
    # closed before any VP round-trip; the keyed upsert in Phase 2 makes the
    # final write idempotent even if map_firm changed in the interim.
    #
    # Why the raw artifact read and NOT S3QueryCollectionOperator: we need
    # EVERY map_firm row materialized into an in-memory dict for branching
    # here. The operator's 'single-row' mode returns only 0/1 row, and its
    # 'dataset' mode opens the artifact with use_lock=True (it writes the
    # result back as a new collection table) and returns a collection-name
    # reference rather than the rows — i.e. it would re-introduce the very
    # lock this phase removes AND force a second read to materialize the rows.
    # The lock-free raw read is the correct tool (and the codebase convention
    # for "read existing map into Python"); writes still go through the
    # canonical S3UpsertCollectionOperator in Phase 2.
    existing_map = {}
    with rail.lib.s3_collection.get_or_create_s3_collection_artifact(
        s3_artifact_name, s3_integration, s3_customer, context,
        integration_type=s3_integration_type, use_lock=False,
    ) as artifact:
        with sqlite3.connect(artifact.local_filename) as conn:
            existing_map = _load_existing_map_firm_index(conn.cursor())

    # ---- Phase 1: all VP API work, accumulate map rows in memory ----
    # One bulk GET serves all per-record QBOID lookups for this run (replaces
    # N sequential VP GETs with one paginated call indexed in memory). Nothing
    # touches S3 here, so the collection lock is NOT held across the VP
    # POST/PUT round-trips.
    vp_firms_by_qboid = _load_vp_firms_by_qboid(vp_conn_id, context)
    log.info(
        "Loaded %d VP firms with QBOID for in-memory lookup",
        len(vp_firms_by_qboid),
    )

    map_rows = []

    def _process_one(qbo_record, is_vendor):
        qbo_id = qbo_record.get('Id')
        display_name = (
            qbo_record.get('DisplayName')
            or qbo_record.get('CompanyName')
            or ''
        )
        is_vendor_flag = 'Y' if is_vendor else 'N'

        if not qbo_id:
            summary['errors'].append({
                'qbo_id': None,
                'name': display_name,
                'error': 'QBO record has no Id field',
            })
            return

        try:
            existing = existing_map.get((str(qbo_id), is_vendor_flag))

            # Local map_firm row absent OR present but unmapped (blank
            # FirmID from a prior unmapped run) → consult the bulk
            # VP-by-QBOID index to see whether VP ALREADY has a firm for
            # this QBO entity. This is purely a mapped-vs-unmapped
            # *discovery*: we never create a VP firm here (see the unmapped
            # branch below). A successful PUT backfills the local map_firm
            # row, so subsequent runs hit the cache and skip the VP index.
            if not existing or not existing.get('FirmID'):
                vp_firm = vp_firms_by_qboid.get(
                    (str(qbo_id), is_vendor_flag)
                )
                if vp_firm and vp_firm.get('ClientID'):
                    existing = {
                        'FirmID': vp_firm['ClientID'],
                        'QBOID': str(qbo_id),
                        'IsVendor': is_vendor_flag,
                        'Name': (
                            vp_firm.get('Name') or display_name
                        ),
                    }
                    log.info(
                        "QBO %s %s (%s): found existing VP firm "
                        "%s by QBOID lookup; switching to PUT.",
                        'vendor' if is_vendor else 'customer',
                        qbo_id, display_name, vp_firm['ClientID'],
                    )

            if existing and existing.get('FirmID'):
                # MAPPED: a VP firm already exists for this QBO entity.
                # Update it (PUT) and, for vendors, upsert
                # VendorAccountingInfo. Mirrors the "update" half of
                # Workato's clendor recipe.
                client_id = existing['FirmID']
                update_body = build_vp_firm_update_body_from_qbo(
                    qbo_record, is_vendor
                )
                VantagepointFirmOperator(
                    task_id=f'_put_firm_{qbo_id}',
                    vp_conn_id=vp_conn_id,
                    request_method='PUT',
                    client_id=client_id,
                    request_body=update_body,
                    pagination=False,
                ).execute(context)
                summary['updated'] += 1

                # Sub-resource: VendorAccountingInfo (vendors only).
                # Workato recipe
                # 014_503_psa_dvp_insert_update_veaccounting is an
                # UPSERT keyed on the firm's existing VEAccounting
                # array:
                #   - empty     -> POST /vision/firm/
                #                  VendorAccountingInfo (create body)
                #   - non-empty -> PUT /api/firm/{ClientID} with
                #                  {"VEAccounting": [{...partial...}]}
                # Always-POST'ing surfaces as a server-side
                # NullReferenceException because VP treats the
                # duplicate insert as a conflict and dereferences a
                # null related entity. A single GET serves both the
                # vendor-code resolution and the upsert decision.
                if is_vendor:
                    vp_vendor_code, existing_ve_accounting, vp_vendor_type = (
                        _resolve_vp_firm_for_veaccounting(
                            client_id, vp_conn_id, context,
                        )
                    )
                    if not vp_vendor_type:
                        # VP rejects every PUT to /api/firm/{ClientID}
                        # (including VEAccounting-only PUTs) when the
                        # firm has no Category set. Backfill it from
                        # the Airflow Variable equivalent before the
                        # VEAccounting upsert — mirrors Workato's
                        # "always have Category set" assumption. If
                        # the Variable is also unset, fall through
                        # and let VP's original "Please provide a
                        # Vendor Type" error surface (actionable
                        # config gap rather than silent skip). See
                        # MAP_FIRM_SYNC_FIX_LOG.md #11.
                        default_vt = lookup_default_vendor_type(instance)
                        if default_vt:
                            VantagepointFirmOperator(
                                task_id=f'_put_category_backfill_{qbo_id}',
                                vp_conn_id=vp_conn_id,
                                request_method='PUT',
                                client_id=client_id,
                                request_body={'Category': default_vt},
                                pagination=False,
                            ).execute(context)
                            log.info(
                                "QBO vendor %s (%s): backfilled VP "
                                "firm %s Category=%s; proceeding "
                                "with VEAccounting upsert.",
                                qbo_id, display_name, client_id,
                                default_vt,
                            )
                            summary['backfilled_vendor_type'] += 1
                            vp_vendor_type = default_vt

                    if existing_ve_accounting:
                        # Update branch: PUT /api/firm/{client_id}
                        # with a nested VEAccounting array.
                        VantagepointFirmOperator(
                            task_id=f'_put_veacct_{qbo_id}',
                            vp_conn_id=vp_conn_id,
                            request_method='PUT',
                            client_id=client_id,
                            request_body=build_ve_accounting_update_body(
                                qbo_record, existing_ve_accounting,
                            ),
                            pagination=False,
                        ).execute(context)
                    elif not vp_vendor_code:
                        # Insert path needs the VP-resolved Vendor
                        # code or it'll NPE. If VP hasn't assigned
                        # one yet, warn and skip — the map_firm row
                        # still gets upserted below, and the
                        # accounting info can be retried out-of-band.
                        log.warning(
                            "QBO vendor %s (%s): VP firm %s has no "
                            "Vendor code and no existing "
                            "VEAccounting; skipping "
                            "VendorAccountingInfo upsert.",
                            qbo_id, display_name, client_id,
                        )
                    else:
                        VantagepointCustomOperator(
                            task_id=f'_post_veacct_{qbo_id}',
                            vp_conn_id=vp_conn_id,
                            endpoint='/vision/firm/VendorAccountingInfo',
                            request_method='POST',
                            pagination=False,
                            request_body=build_ve_accounting_body(
                                qbo_record, vp_vendor_code,
                            ),
                        ).execute(context)

                # Accumulate the cross-reference row with the resolved
                # VP ClientID (written in the Phase 2 batched upsert).
                map_rows.append(_build_map_firm_row(
                    client_id, str(qbo_id), is_vendor_flag, display_name,
                ))
            else:
                # UNMAPPED: no VP firm exists for this QBO entity.
                # Workato parity — DO NOT create one. Workato's
                # synch_firms only fills ClientID for entities that
                # already resolve to a VP firm (recipe step 11
                # filters `WHERE mf.QBOID != '' AND mf.ClientID =
                # ''` and the clendor recipe is gated so QBO-native
                # / sample entities stay unmapped). Record the
                # cross-reference with a blank FirmID so the QBO
                # entity is tracked but left unmapped, exactly like
                # Workato leaves FirmID empty.
                #
                # History: this branch previously POST'd a new VP
                # firm (+ address/contact sub-resources) for EVERY
                # active QBO customer/vendor. That polluted VP with
                # firms Workato never created and populated FirmID
                # where Workato left it blank. The create-body
                # builders (`build_vp_firm_create_body_from_qbo`
                # etc.) are retained for a potential explicit
                # reverse/create flow but are intentionally not
                # invoked by the forward sync.
                map_rows.append(_build_map_firm_row(
                    '', str(qbo_id), is_vendor_flag, display_name,
                ))
                summary['unmapped'] += 1
                log.info(
                    "QBO %s %s (%s): no VP firm found; recorded "
                    "map_firm row with blank FirmID (unmapped).",
                    'vendor' if is_vendor else 'customer',
                    qbo_id, display_name,
                )

        except Exception as exc:  # pylint: disable=broad-exception-caught
            log.error(
                "Failed to sync QBO %s %s (%s): %s",
                'vendor' if is_vendor else 'customer',
                qbo_id, display_name, exc,
            )
            summary['errors'].append({
                'qbo_id': qbo_id,
                'name': display_name,
                'error': str(exc),
            })

    for customer in customers:
        _process_one(customer, is_vendor=False)
    for vendor in vendors:
        _process_one(vendor, is_vendor=True)

    # ---- Phase 2: single batched upsert (one S3 lock cycle) ----
    # All accumulated rows go up in ONE download/modify/upload/lock cycle via
    # the canonical S3 collection operator, keyed on (QBOID, IsVendor). The
    # old shape held the collection open and locked across every VP HTTP call;
    # this confines the lock to the batched write.
    if map_rows:
        S3UpsertCollectionOperator(
            task_id='_upsert_map_firm',
            integration=s3_integration,
            customer=s3_customer,
            integration_type=s3_integration_type,
            collection_name=MAP_FIRM_TABLE_NAME,
            key_columns=MAP_FIRM_UNIQUE_COLUMNS,
            rows=map_rows,
        ).execute(context)
        log.info("Upserted %d map_firm row(s) in one S3 cycle.", len(map_rows))
    else:
        log.info("No map_firm rows to upsert.")

    log.info("map_firm forward-sync summary: %s", summary)
    # If any per-record failure occurred, raise so the dag's catch task fires
    # and the dispatcher surfaces the failure. Successful records have already
    # been committed to S3 above.
    if summary['errors']:
        raise RuntimeError(
            f"map_firm sync had {len(summary['errors'])} failure(s); "
            f"first: {summary['errors'][0]}"
        )
    return summary


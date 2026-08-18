"""
Common utility methods for VP QBO Vendor Sync integration.
"""
import logging
import rail
from vp_quickbooks_integration.vendor_sync.config import default_pay_terms
# IntegrationConfig is used for CFG_DefaultVendorType (get_cfg) + the
# missing-table error message. The shared collection helpers (+ the mapping_sync
# integration_type constant) and the table/column/pay-terms constants come from
# common so the S3 access logic and SQLite identifiers can't drift.
from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig
from vp_quickbooks_integration.common.python_callable_method import (
    collection_rows,
    collection_upsert,
    read_lookup_variable,
    MAPPING_COLLECTION_INTEGRATION_TYPE,
)
from vp_quickbooks_integration.common.tables import (
    PAY_TERMS_MAP,
    MAP_FIRM_TABLE_NAME,
    MAP_FIRM_COLUMNS,
    MAP_FIRM_UNIQUE_COLUMNS,
)

logger = logging.getLogger(__name__)


def lookup_pay_terms(qbo_term_ref):
    """QBO Term Id (TermRef.value) -> VP PayTerms via PAY_TERMS_MAP. None if unmapped."""
    if not qbo_term_ref:
        return None
    return PAY_TERMS_MAP.get(str(qbo_term_ref))


def lookup_default_vendor_type(instance):
    """Default Category for new VP firms.

    Ports the Workato account property `CFG_DefaultVendorType`. Resolved
    CFG-first from the middleware integration payload
    (`dag_run.conf['config']['CFG_DefaultVendorType']`, via
    IntegrationConfig.get_cfg), falling back to the legacy per-instance Variable
    `vp_qbo_vendor_sync_default_vendor_type_{instance}` for backwards
    compatibility.
    """
    try:
        context = rail.get_current_context()
    except Exception:  # pylint: disable=broad-exception-caught
        context = None
    if context is not None:
        value = IntegrationConfig.get_cfg(context, 'CFG_DefaultVendorType')
        if value:
            return value
    return read_lookup_variable(
        f'vp_qbo_vendor_sync_default_vendor_type_{instance}',
        default=None
    )


# Collection access for the shared `map_firm` table (read in the router, write
# on create + update-fallback) uses the shared helpers in
# common.python_callable_method — `collection_rows` / `collection_upsert`,
# imported above and called directly. map_firm carries a UNIQUE index on
# (QBOID, IsVendor) (MAP_FIRM_UNIQUE_COLUMNS), so writes use a single atomic
# upsert keyed on those columns (see _write_map_firm_row).


def _write_map_firm_row(firm_id, qbo_id, name):
    """Upsert a vendor row (IsVendor='Y') in the shared map_firm collection via
    a single atomic S3UpsertCollectionOperator call — same RAIL-operator
    standard as the S3QueryCollectionOperator reads.

    Keyed on MAP_FIRM_UNIQUE_COLUMNS (QBOID, IsVendor): ``INSERT ... ON
    CONFLICT(QBOID, IsVendor) DO UPDATE`` replaces the existing vendor row's
    non-key columns in ONE S3 download->mutate->upload commit (dup-free,
    idempotent, atomic). This supersedes the old DELETE-then-INSERT idiom — that
    was only needed back when map_firm had no UNIQUE index; mapping_sync's
    init_mapping_collections now creates the UNIQUE(QBOID, IsVendor) index from
    MAP_FIRM_UNIQUE_COLUMNS, so the single-statement atomic upsert is available
    and the inter-commit "deleted-but-not-reinserted" window is gone.

    The table itself is created by mapping_sync; if it's absent (mapping_sync
    hasn't run for this customer) the write FAILS LOUDLY with an actionable
    error rather than leaving the just-created VP firm unmapped (which would
    make the next sync create a duplicate). Mirrors mapping_sync's helper
    pattern (utils/_shared.py)."""
    context = rail.get_current_context()
    values = {
        'FirmID': str(firm_id),
        'QBOID': str(qbo_id),
        'IsVendor': 'Y',
        'Name': name,
    }

    try:
        # Atomic upsert keyed on (QBOID, IsVendor): inserts the cross-reference
        # row, or replaces the existing vendor row's non-key columns on re-sync.
        collection_upsert(
            MAP_FIRM_TABLE_NAME,
            MAP_FIRM_UNIQUE_COLUMNS,
            {c: values[c] for c in MAP_FIRM_COLUMNS},
            context,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # A missing collection / map_firm table is the one case we translate
        # into an actionable error. Unlike the reads (where 'not found'
        # legitimately means 'route to create'), the write MUST land: the firm
        # is already live in VP, so a silently-unwritten mapping makes the next
        # sync create a DUPLICATE. mapping_sync owns creating the collection +
        # table per customer — surface that clearly instead of a raw
        # FileNotFoundError / 'no such table'. (The write goes through
        # get_or_create_s3_collection_artifact, which auto-creates an empty
        # collection, so a missing collection usually surfaces as 'no such
        # table' rather than FileNotFoundError — handle both.)
        if isinstance(exc, FileNotFoundError) or 'no such table' in str(exc).lower():
            customer = IntegrationConfig.get_s3_customer(context)
            raise RuntimeError(
                f"map_firm write failed for customer '{customer}' "
                f"(integration_type='{MAPPING_COLLECTION_INTEGRATION_TYPE}'): "
                f"the mapping_sync collection / '{MAP_FIRM_TABLE_NAME}' table is "
                f"missing. The firm is already created in VP "
                f"(FirmID={firm_id}, QBOID={qbo_id}) but its mapping could not "
                f"be persisted — run mapping_sync for this customer first, then "
                f"re-sync, to avoid creating a duplicate firm."
            ) from exc
        raise


def add_firm_to_firm_map():
    """Insert/replace the map_firm row for this vendor after a successful create.

    Mirrors the Workato 'insert_entry on 014-503 PSA Map Firm' step, now
    writing the shared mapping_sync S3 collection instead of an Airflow Variable.
    FirmID comes from the upstream capture_client_id task XCom.
    """
    conf = rail.get_current_context()['dag_run'].conf
    qbo_id = conf.get('Id')
    firm_id = rail.result('capture_client_id')

    if not qbo_id or not firm_id:
        logger.warning(
            "Skipping firm map write — missing key field "
            "(qbo_id=%s, firm_id=%s)", qbo_id, firm_id
        )
        return None

    _write_map_firm_row(firm_id, qbo_id, conf.get('DisplayName'))
    logger.info("Added map_firm entry: QBOID %s -> FirmID %s", qbo_id, firm_id)
    return firm_id


# ---------------------------------------------------------------------------
# Router (router_dag) helpers
# ---------------------------------------------------------------------------

def lookup_firm_by_qboid():
    """Find an existing firm row in the shared map_firm collection by QBOID.

    Ports the Workato 'PSA Firm QBOID Exists' lookup to the mapping_sync S3
    collection. Returns the row dict {FirmID, QBOID, IsVendor, Name} or None.
    Vendors are matched with IsVendor='Y'. A missing collection / table (e.g.
    a brand-new customer mapping_sync hasn't populated) is treated as
    'not found', so the router falls through to the create path.
    """
    qbo_id = rail.get_current_context()['dag_run'].conf.get('Id')
    if not qbo_id:
        return None

    # A missing collection / table (mapping_sync hasn't populated this customer
    # yet) is treated as 'not found' by collection_rows (returns []), so the
    # router falls through to the create path.
    rows = collection_rows(
        MAP_FIRM_TABLE_NAME,
        MAP_FIRM_COLUMNS,
        "QBOID = ? AND IsVendor = 'Y'",
        [str(qbo_id)],
    )
    return rows[0] if rows else None


def has_vp_client_id_in_conf(**context):
    """IfOperator test for vendor_update_dag.

    Mirrors recipe step 13: `present(FirmID)` inside the update branch.
    If conf.vp_client_id is empty (firm map row was stale), we fall
    through to the create-fallback path inside the update DAG.
    """
    return bool(context['dag_run'].conf.get('vp_client_id'))


def capture_client_id_from_fallback():
    """Extract ClientID from create_firm_in_vp_fallback response."""
    response = rail.result('create_firm_in_vp_fallback')
    if isinstance(response, list) and response:
        return (response[0] or {}).get('ClientID')
    if isinstance(response, dict):
        return response.get('ClientID')
    return None


def add_firm_to_firm_map_fallback():
    """Patch the map_firm row after a fallback create (recipe step 21).

    Mirrors Workato `update_entry` on `014-503 PSA Map Firm` — replaces the
    stale FirmID in the existing QBOID row with the newly-created ClientID.
    Same body as `add_firm_to_firm_map` (DELETE-then-INSERT naturally replaces
    the stale row), but reads the ClientID from the `create_firm_in_vp_fallback`
    task XCom instead of `capture_client_id`.
    """
    conf = rail.get_current_context()['dag_run'].conf
    qbo_id = conf.get('Id')
    firm_id = capture_client_id_from_fallback()

    if not qbo_id or not firm_id:
        logger.warning(
            "Skipping firm map fallback write — missing key field "
            "(qbo_id=%s, firm_id=%s)", qbo_id, firm_id
        )
        return None

    _write_map_firm_row(firm_id, qbo_id, conf.get('DisplayName'))
    logger.info(
        "Patched map_firm entry (fallback): QBOID %s -> FirmID %s",
        qbo_id, firm_id
    )
    return firm_id


def resolve_firm_id_for_update():
    """Return the FirmID downstream update_dag tasks should use.

    Normal update path → conf.vp_client_id (set by router from firm map row).
    Fallback path (firm map row was stale) → ClientID from the fallback
    POST response.
    """
    conf = rail.get_current_context()['dag_run'].conf
    vp_id = conf.get('vp_client_id')
    if vp_id:
        return vp_id
    return capture_client_id_from_fallback()


def check_firm_exists_in_lookup():
    """IfOperator test: did get_firm_from_lookup return a row?"""
    return rail.result('get_firm_from_lookup') is not None


def build_vendor_conf(operation_type):
    """Build conf for the vendor_create / vendor_update child DAG."""
    conf = rail.get_current_context()['dag_run'].conf
    result = {
        **conf,
        'type': operation_type,
        'connections': conf.get('connections')
    }
    if operation_type == 'update':
        firm_row = rail.result('get_firm_from_lookup')
        if firm_row:
            result['vp_client_id'] = firm_row.get('FirmID')
    return result


def collect_triggered_dagrun_ids():
    """Collect dag run(s) from whichever trigger executed (create or update)."""
    dag_runs = []
    for task_id in ['trigger_vendor_create', 'trigger_vendor_update']:
        try:
            result = rail.result(task_id)
            if result is not None:
                dag_runs.append(result)
        except Exception:  # pylint: disable=broad-exception-caught
            pass
    return dag_runs


# ---------------------------------------------------------------------------
# Body builders
# ---------------------------------------------------------------------------

def _qbo_status_to_vp(active):
    """QBO Active (bool) -> VP Status code."""
    if active is True:
        return 'A'
    if active is False:
        return 'I'
    return None


def _yes_no(value):
    """Bool -> 'Y'/'N'. None passes through."""
    if value is True:
        return 'Y'
    if value is False:
        return 'N'
    return None


def _filter_none(body):
    return {k: v for k, v in body.items() if v is not None}


_BILLING_ADDRESS_TYPE = 'Billing'


def build_create_firm_body(instance):
    """Firm body for POST /firm (new vendor). Mirrors Workato CREATE step.

    Notes vs. Workato recipe:
    - `Vendor` is set to QBOID (recipe parity; tenant autonumber on Vendor
      has been disabled so override is allowed). This makes downstream
      VEAccounting calls trivial — Vendor reference is just QBOID.
    - `Client` is NOT sent (autonumber on the Client / firm-number field
      is still active in this tenant).
    - Org / Type are omitted (recipe set them to ''); empty strings caused
      "Please provide a Relationship for table Firm" in this tenant.
      VendorInd='Y' establishes the vendor relationship.
    - Name / SortName use QBO `CompanyName` when present, falling back to
      `DisplayName` (mirrors Workato `QBCompany = Company.present? ? Company
      : DisplayName`).
    """
    conf = rail.get_current_context()['dag_run'].conf
    qbo_id = conf.get('Id')
    web_addr = conf.get('WebAddr') or {}
    qb_company = conf.get('CompanyName') or conf.get('DisplayName')

    body = {
        'QBOID': qbo_id,
        'Vendor': qbo_id,
        'Name': qb_company,
        'SortName': qb_company,
        'Status': _qbo_status_to_vp(conf.get('Active')),
        'WebSite': web_addr.get('URI'),
        'VendorInd': 'Y',
        'ClientInd': 'N',
        'Category': lookup_default_vendor_type(instance),
        'ExportInd': False,
        'PriorWork': False,
        'Recommend': False,
        'GovernmentAgency': False,
        'Competitor': False,
        'AvailableForCRM': 'N',
        'ReadyForApproval': False,
        'ReadyForProcessing': 'N'
    }
    return _filter_none(body)


def build_update_firm_body():
    """Firm body for PUT /firm/{ClientID} (existing vendor).

    Per Workato recipe, Category is intentionally NOT updated (Category=skip),
    so it's omitted here. PrimaryEmail is added on update only.

    `Vendor` is set to QBOID (recipe parity). `Client` / Org / Type are
    omitted — autonumber + relationship constraints in this VP tenant.
    Name / SortName use the QBCompany fallback (CompanyName -> DisplayName).
    """
    conf = rail.get_current_context()['dag_run'].conf
    qbo_id = conf.get('Id')
    web_addr = conf.get('WebAddr') or {}
    primary_email = conf.get('PrimaryEmailAddr') or {}
    qb_company = conf.get('CompanyName') or conf.get('DisplayName')

    body = {
        'QBOID': qbo_id,
        'Vendor': qbo_id,
        'Name': qb_company,
        'SortName': qb_company,
        'Status': _qbo_status_to_vp(conf.get('Active')),
        'WebSite': web_addr.get('URI'),
        'PrimaryEmail': primary_email.get('Address'),
        'VendorInd': 'Y',
        'ClientInd': 'N',
        'ExportInd': False,
        'PriorWork': False,
        'Recommend': False,
        'GovernmentAgency': False,
        'Competitor': False,
        'AvailableForCRM': 'N',
        'ReadyForApproval': False,
        'ReadyForProcessing': 'N'
    }
    return _filter_none(body)


# ---------------------------------------------------------------------------
# Address (firm_address) helpers
#
# Recipe `014-503 PSA Vantagepoint Upsert Firm Address` flow:
#   1. GET /firm/{ClientID}/address (list existing)
#   2. Loop: if Address.upcase == input.Address.upcase -> PUT firm with
#      CLAddress array (uses CLAddressID + QBOIsBillingAddr/QBOIsShippingAddr)
#   3. After loop, if no match -> POST /firm/{ClientID}/address (PrimaryInd,
#      Payment, Billing, Address fields)
# ---------------------------------------------------------------------------

def _qbo_address_inputs():
    """Pull QBO BillAddr + contact fields from conf into a flat dict."""
    conf = rail.get_current_context()['dag_run'].conf
    bill_addr = conf.get('BillAddr') or {}
    primary_phone = conf.get('PrimaryPhone') or {}
    fax = conf.get('Fax') or {}
    primary_email = conf.get('PrimaryEmailAddr') or {}
    return {
        'qbo_id': conf.get('Id'),
        'Address1': bill_addr.get('Line1'),
        'Address2': bill_addr.get('Line2'),
        'Address3': bill_addr.get('Line3'),
        'City': bill_addr.get('City'),
        'State': bill_addr.get('CountrySubDivisionCode'),
        'Zip': bill_addr.get('PostalCode'),
        'Country': bill_addr.get('Country'),
        'Phone': primary_phone.get('FreeFormNumber'),
        'FAX': fax.get('FreeFormNumber'),
        'Email': primary_email.get('Address')
    }


def has_any_billing_address():
    """Recipe gate (step 22): only call address upsert if at least one of the
    7 billing-address fields is present. Mirrors Workato recipe `OR` over:
    BillingAddressLine1/2/3, BillingCity, BillingState, BillingZip,
    BillingCountry.
    """
    conf = rail.get_current_context()['dag_run'].conf
    bill = conf.get('BillAddr') or {}
    return any([
        bill.get('Line1'),
        bill.get('Line2'),
        bill.get('Line3'),
        bill.get('City'),
        bill.get('CountrySubDivisionCode'),
        bill.get('PostalCode'),
        bill.get('Country')
    ])


def has_first_and_last_name():
    """Recipe gate (step 29): only call contact upsert if both FirstName
    (QBO `GivenName`) and LastName (QBO `FamilyName`) are present.
    """
    conf = rail.get_current_context()['dag_run'].conf
    return bool(conf.get('GivenName')) and bool(conf.get('FamilyName'))


def has_pay_terms_input():
    """Recipe gate (step 34): finalize firm with ReadyForProcessing='Y'
    only when QBO supplied a `TermRef.value`.
    """
    conf = rail.get_current_context()['dag_run'].conf
    term_ref = conf.get('TermRef') or {}
    return bool(term_ref.get('value'))


def has_suffix_input():
    """Contact recipe gate (step 4): only run CFGSuffix lookup if Suffix
    was supplied.
    """
    conf = rail.get_current_context()['dag_run'].conf
    return bool(conf.get('Suffix'))


def is_suffix_in_codetable():
    """Contact recipe step 7-8 logic: return True if input.Suffix already
    exists in CFGSuffix code table (`Code` column match).
    """
    suffix_codes = rail.result('get_suffix_codes_from_vp')
    target = (
        rail.get_current_context()['dag_run'].conf.get('Suffix')
    )
    if not target or not isinstance(suffix_codes, list):
        return False
    return any(
        isinstance(entry, dict) and entry.get('Code') == target
        for entry in suffix_codes
    )


def build_create_suffix_body():
    """Recipe step 9: POST /codeTable/CFGSuffix with the new code.
    Description equals Code (recipe sends both with the same Suffix value).
    """
    suffix = rail.get_current_context()['dag_run'].conf.get('Suffix')
    return {'Code': suffix, 'Description': suffix}


def _resolve_firm_client_id():
    """Get firm ClientID for context-aware use (works in both create
    and update DAGs). Tries in order:
      1. `resolve_firm_id_for_update` task (update DAG flow)
      2. `capture_client_id` task (create DAG flow)
      3. `create_firm_in_vp` response (fallback)
    """
    for task_id in ('resolve_firm_id_for_update', 'capture_client_id'):
        try:
            value = rail.result(task_id)
            if value:
                return value
        except Exception:  # pylint: disable=broad-exception-caught
            continue
    try:
        return _firm_record_from_response('create_firm_in_vp').get('ClientID')
    except Exception:  # pylint: disable=broad-exception-caught
        return None


def _firm_record_from_response(task_id):
    """Generic firm-record extractor for any task whose XCom holds the
    firm response (list or dict).
    """
    try:
        response = rail.result(task_id)
    except Exception:  # pylint: disable=broad-exception-caught
        return {}
    if isinstance(response, list) and response:
        return response[0] or {}
    if isinstance(response, dict):
        return response
    return {}


def build_create_contact_body():
    """POST body for /contact (new firm contact).

    Mirrors Workato `014-503 PSA QuickBooks Contact to Vantagepoint`
    create branch (recipe step 16). FirmAddressDescription defaults to
    'Billing' (the only address type vendor_sync handles); set to None
    when no billing address was upserted so it gets filtered out.
    """
    conf = rail.get_current_context()['dag_run'].conf
    client_id = _resolve_firm_client_id()
    primary_phone = conf.get('PrimaryPhone') or {}
    fax = conf.get('Fax') or {}
    mobile = conf.get('Mobile') or {}
    primary_email = conf.get('PrimaryEmailAddr') or {}
    web_addr = conf.get('WebAddr') or {}

    firm_address_desc = (
        _BILLING_ADDRESS_TYPE if has_any_billing_address() else None
    )

    body = {
        'ClientID': client_id,
        'ContactStatus': 'A',
        'FirstName': conf.get('GivenName'),
        'MiddleName': conf.get('MiddleName'),
        'LastName': conf.get('FamilyName'),
        'Suffix': conf.get('Suffix'),
        'Title': conf.get('Title'),
        'Phone': primary_phone.get('FreeFormNumber'),
        'Fax': fax.get('FreeFormNumber'),
        'CellPhone': mobile.get('FreeFormNumber'),
        'Email': primary_email.get('Address'),
        'Website': web_addr.get('URI'),
        'QBOID': conf.get('Id'),
        'QBOIsMainContact': 'true',
        'FirmAddressDescription': firm_address_desc
    }
    return _filter_none(body)


# ---------------------------------------------------------------------------
# Contact update helpers (recipe steps 10-14)
# ---------------------------------------------------------------------------

def build_firm_contacts_filter():
    """Filter string for GET /contact to list contacts attached to the
    current firm. Reads ClientID from `resolve_firm_id_for_update` XCom.
    """
    client_id = _resolve_firm_client_id()
    return (
        f'?filterHash[0][name]=ClientID&'
        f'filterHash[0][value]={client_id}'
    )


def _unwrap_contacts_response(raw):
    """Normalize contact GET response to a list of contact dicts."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ('rows', 'Contact', 'contacts', 'Body', 'body', 'array'):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def find_matching_contact_id():
    """Find an existing contact's ContactID by matching FirstName,
    MiddleName, LastName (recipe step 12 — SQL query in smart_list).

    Reads from `get_firm_contacts` task XCom. Returns ContactID or None.
    Empty strings vs missing fields are normalized (both treated as '').
    """
    contacts = _unwrap_contacts_response(rail.result('get_firm_contacts'))
    conf = rail.get_current_context()['dag_run'].conf
    target_first = (conf.get('GivenName') or '').strip()
    target_middle = (conf.get('MiddleName') or '').strip()
    target_last = (conf.get('FamilyName') or '').strip()
    if not target_first or not target_last:
        return None
    for c in contacts:
        if not isinstance(c, dict):
            continue
        if (
            (c.get('FirstName') or '').strip() == target_first
            and (c.get('MiddleName') or '').strip() == target_middle
            and (c.get('LastName') or '').strip() == target_last
        ):
            return c.get('ContactID')
    return None


def check_matching_contact_exists():
    """IfOperator test: did find_matching_contact_id return a ContactID?"""
    return bool(rail.result('find_matching_contact_id'))


def build_update_contact_body():
    """PUT body for /contact/{ContactID} (update existing contact).

    Mirrors Workato recipe step 14. Same fields as the POST body except:
    - Includes ContactID
    - Omits QBOIsMainContact (set only on create per recipe)
    """
    conf = rail.get_current_context()['dag_run'].conf
    contact_id = rail.result('find_matching_contact_id')
    client_id = _resolve_firm_client_id()
    primary_phone = conf.get('PrimaryPhone') or {}
    fax = conf.get('Fax') or {}
    mobile = conf.get('Mobile') or {}
    primary_email = conf.get('PrimaryEmailAddr') or {}
    web_addr = conf.get('WebAddr') or {}

    firm_address_desc = (
        _BILLING_ADDRESS_TYPE if has_any_billing_address() else None
    )

    body = {
        'ContactID': contact_id,
        'ClientID': client_id,
        'ContactStatus': 'A',
        'FirstName': conf.get('GivenName'),
        'MiddleName': conf.get('MiddleName'),
        'LastName': conf.get('FamilyName'),
        'Suffix': conf.get('Suffix'),
        'Title': conf.get('Title'),
        'Phone': primary_phone.get('FreeFormNumber'),
        'Fax': fax.get('FreeFormNumber'),
        'CellPhone': mobile.get('FreeFormNumber'),
        'Email': primary_email.get('Address'),
        'Website': web_addr.get('URI'),
        'QBOID': conf.get('Id'),
        'FirmAddressDescription': firm_address_desc
    }
    return _filter_none(body)


def build_create_firm_address_body():
    """POST body for /firm/{ClientID}/address (new address record).

    Includes PrimaryInd / Payment flags and Billing/Address type label.
    """
    addr = _qbo_address_inputs()
    is_billing = 'Y'
    body = {
        'Address': _BILLING_ADDRESS_TYPE,
        'PrimaryInd': 'true',
        'Payment': 'true',
        'Billing': is_billing,
        'Address1': addr['Address1'],
        'Address2': addr['Address2'],
        'Address3': addr['Address3'],
        'City': addr['City'],
        'State': addr['State'],
        'Zip': addr['Zip'],
        'Country': addr['Country'],
        'Phone': addr['Phone'],
        'FAX': addr['FAX'],
        'Email': addr['Email']
    }
    return _filter_none(body)


def _unwrap_address_response(raw):
    """Normalize firm-address GET response to a list."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ('addresses', 'CLAddress', 'Body', 'body', 'array'):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def find_billing_address_id():
    """Find CLAddressID of the existing Billing-type address.

    Reads from `get_firm_addresses` task XCom. Returns the CLAddressID
    string or None if no Billing address exists. Comparison is
    case-insensitive (recipe matches via `.upcase`).
    """
    addresses = _unwrap_address_response(rail.result('get_firm_addresses'))
    target = _BILLING_ADDRESS_TYPE.upper()
    for addr in addresses:
        if not isinstance(addr, dict):
            continue
        addr_type = (addr.get('Address') or '').upper()
        if addr_type == target:
            return addr.get('CLAddressID')
    return None


def check_billing_address_exists():
    """IfOperator test: does a BILLING address already exist on the firm?"""
    return rail.result('find_billing_address_id') is not None


def build_update_firm_address_body():
    """PUT body for /firm/{ClientID} with CLAddress array (update existing).

    Reads CLAddressID from `find_billing_address_id` XCom and ClientID from
    `resolve_firm_id_for_update` XCom (which prefers conf.vp_client_id but
    falls back to the fallback-create response when the firm map row was
    stale).
    """
    cl_address_id = rail.result('find_billing_address_id')
    addr = _qbo_address_inputs()
    client_id = rail.result('resolve_firm_id_for_update')
    record = {
        'CLAddressID': cl_address_id,
        'ClientID': client_id,
        'Address': _BILLING_ADDRESS_TYPE,
        'Billing': 'Y',
        'QBOIsBillingAddr': 'Y',
        'QBOIsShippingAddr': 'N',
        'Address1': addr['Address1'],
        'Address2': addr['Address2'],
        'Address3': addr['Address3'],
        'City': addr['City'],
        'State': addr['State'],
        'Zip': addr['Zip'],
        'Country': addr['Country'],
        'Phone': addr['Phone'],
        'FAX': addr['FAX'],
        'Email': addr['Email'],
        'QBOID': addr['qbo_id']
    }
    return {'CLAddress': [_filter_none(record)]}


# ---------------------------------------------------------------------------
# VEAccounting helpers
#
# Recipe `014-503 PSA DVP Insert Update VEAccounting` flow:
#   1. PayTerms: lookup col1 -> col2 in pay-terms map; default 'Next'
#   2. GET /api/firm/{ClientID}/vendorAccountingInfo
#   3. If body.array has Vendor entries -> PUT /api/firm/{ClientID} with
#      {VEAccounting: [{PayTerms, Req1099}]}
#   4. Else -> POST /vision/firm/VendorAccountingInfo/ with full body
# ---------------------------------------------------------------------------

def _unwrap_veaccounting_response(raw):
    """VP can return either a bare list or {array: [...]}/{Body: [...]}.
    Normalize to a list of records.
    """
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ('array', 'Body', 'body', 'VEAccounting'):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def check_veaccounting_exists():
    """IfOperator test: does VEAccounting already have any record?

    Reads from `get_firm_veaccounting` XCom (array of records).
    """
    records = _unwrap_veaccounting_response(
        rail.result('get_firm_veaccounting')
    )
    return any(r.get('Vendor') for r in records if isinstance(r, dict))


def _resolve_vp_vendor_code():
    """VP firm Vendor code, read dynamically from a firm fetch.

    Even though firm POST sends `Vendor: QBOID`, we don't ASSUME the
    stored value matches — we read what VP actually has on the firm:
      - create flow: `get_firm_after_create` task XCom (GET /firm/{ClientID})
      - update flow normal path: `update_firm_in_vp` task XCom (PUT response)
      - update flow fallback path: `get_firm_after_create_fallback` task XCom
        (GET after the fallback POST when the firm map row was stale)
    """
    for task_id in (
        'get_firm_after_create',
        'get_firm_after_create_fallback',
        'update_firm_in_vp',
    ):
        try:
            response = rail.result(task_id)
        except Exception:  # pylint: disable=broad-exception-caught
            continue
        firm_record = {}
        if isinstance(response, list) and response:
            firm_record = response[0] or {}
        elif isinstance(response, dict):
            firm_record = response
        vendor = firm_record.get('Vendor')
        if vendor:
            return vendor
    return None


def build_create_veaccounting_body():
    """POST body for /vision/firm/VendorAccountingInfo (new VEAccounting).

    `Vendor` references the firm by VP-stored Vendor code (read dynamically
    from `get_firm_after_create` task XCom).

    `Company` is intentionally NOT sent. Empty string was rejected by VP
    ("Cannot insert NULL into Company column"), and the recipe's
    `=blank` is just Workato shorthand. Omitting lets VP's server-side
    default apply.
    """
    conf = rail.get_current_context()['dag_run'].conf
    term_ref = conf.get('TermRef') or {}
    pay_terms = (
        lookup_pay_terms(term_ref.get('value')) or default_pay_terms
    )
    return {
        'Vendor': _resolve_vp_vendor_code(),
        'PayTerms': pay_terms,
        'PayTermsDesc': pay_terms,
        'Req1099': _yes_no(conf.get('Vendor1099')) or 'N',
        'DiscPct': 0,
        'DiscPeriod': 0,
        'ThisYear1099': 0,
        'LastYear1099': 0,
        'CheckPerVoucher': 'N',
        'MemoPrintOnCheck': 'N',
        'EFTAddenda': 'N',
        'EFTRemittance': 'N',
        'EFTClieOp': 'N',
        'ElectronicPaymentMethodID': ''
    }


def build_update_veaccounting_body():
    """PUT body for /api/firm/{ClientID} with VEAccounting array.

    Recipe sends only PayTerms and Req1099 on update.
    If QBO TermRef is blank, falls back to existing PayTerms from
    `get_firm_veaccounting` XCom (Workato recipe behavior).
    """
    conf = rail.get_current_context()['dag_run'].conf
    term_ref = conf.get('TermRef') or {}
    pay_terms = lookup_pay_terms(term_ref.get('value'))

    if not pay_terms:
        existing = _unwrap_veaccounting_response(
            rail.result('get_firm_veaccounting')
        )
        for record in existing:
            if isinstance(record, dict) and record.get('PayTerms'):
                pay_terms = record.get('PayTerms')
                break

    if not pay_terms:
        pay_terms = default_pay_terms

    return {
        'VEAccounting': [{
            'PayTerms': pay_terms,
            'Req1099': _yes_no(conf.get('Vendor1099')) or 'N'
        }]
    }


# ---------------------------------------------------------------------------
# Post-firm-create capture
# ---------------------------------------------------------------------------

def capture_client_id_from_create():
    """Pull ClientID out of the create_firm_in_vp response."""
    response = rail.result('create_firm_in_vp')
    if isinstance(response, list) and response:
        return response[0].get('ClientID')
    if isinstance(response, dict):
        return response.get('ClientID')
    return None


# ---------------------------------------------------------------------------
# Error capture (return dict; do NOT raise — keeps DAG SUCCESS so parent
# WaitForDagRunsSensor never sees a failed run)
# ---------------------------------------------------------------------------

def _format_vendor_label(qbo_vendor_id, firm_name):
    """Format the vendor identifier prefix for error messages.

    If firm_name is present (non-empty, non-whitespace), output looks like
    `Vendor 56 (Acme Corp)`. Otherwise just `Vendor 56`.
    """
    if firm_name and str(firm_name).strip():
        return f"Vendor {qbo_vendor_id} ({str(firm_name).strip()})"
    return f"Vendor {qbo_vendor_id}"


def capture_create_error(qbo_vendor_id, firm_name, error_message):
    return {
        'error': (
            f"{_format_vendor_label(qbo_vendor_id, firm_name)} - "
            f"create failed: {error_message}"
        )
    }


def capture_update_error(qbo_vendor_id, firm_name, error_message):
    return {
        'error': (
            f"{_format_vendor_label(qbo_vendor_id, firm_name)} - "
            f"update failed: {error_message}"
        )
    }


def capture_router_dag_error(
    qbo_vendor_id, firm_name, fallback_error_message
):
    """Aggregate child errors; fall back to local message; return dict or None.

    Mirrors employee_sync's capture_router_dag_error pattern.
    """
    child_errors = []
    try:
        gathered = rail.result('gather_vendor_dag_errors')
        if gathered:
            child_errors = (
                gathered if isinstance(gathered, list) else [gathered]
            )
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    if child_errors:
        error_message = ' | '.join(
            e.get('error', str(e)) for e in child_errors if e
        )
    elif fallback_error_message:
        error_message = (
            f"{_format_vendor_label(qbo_vendor_id, firm_name)} - "
            f"sync failed: {fallback_error_message}"
        )
    else:
        return None

    return {'error': error_message}


# Watermark helpers (sanitize_customer_id, build_watermark_variable_key,
# utc_now_iso, prepare_sync_timestamps, update_last_sync_time) now live in
# common.python_callable_method; the dispatcher imports them from there.

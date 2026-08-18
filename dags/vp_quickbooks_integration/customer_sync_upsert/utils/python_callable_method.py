"""
Common utility methods for VP -> QBO Customer Upsert integration.

Direction: Vantagepoint PSA Firm -> QuickBooks Online Customer.
Mirrors `vendor_sync/utils/python_callable_method.py` patterns but reversed:
firm map is keyed by VP FirmID (ClientID) rather than QBOID.
"""
import logging
import re
import rail
from airflow.models import Variable
from vp_quickbooks_integration.common.tables import MAP_FIRM_TABLE_NAME as map_firm_table_name
from vp_quickbooks_integration.common.python_callable_method import (
    collection_integration,
    collection_single_row,
    collection_upsert,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Firm map — backed by the shared mapping_sync S3 collection (NOT a Variable).
#
# customer_sync_upsert reads/writes the `map_firm` table inside the
# per-customer SQLite-in-S3 collection that the `mapping_sync` DAG creates.
# Rows use IsVendor='N' (customer side). FirmID is the lookup key (VP->QBO
# direction, reversed from vendor_sync which keys on QBOID).
# See customer_sync_upsert/config.py for S3 path rationale.
# Callers expect the S3 row dict with column 'QBOID' (the QBO Customer Id).
# ---------------------------------------------------------------------------

_INSTANCE_PATTERN = re.compile(r'^[A-Za-z0-9_-]+$')


def _instance_from_dag_id():
    """Extract `{instance}` from the running DAG id.

    DAG ids follow `vp_qbo_customer_upsert_<leaf>_<instance>` (e.g.
    `vp_qbo_customer_upsert_router_dev`). Returns the trailing token,
    validated to a conservative charset so a malformed dag id can't
    inject characters into the Variable name.
    """
    ctx = rail.get_current_context()
    dag = ctx.get('dag')
    dag_id = getattr(dag, 'dag_id', '') or ''
    instance = dag_id.rsplit('_', 1)[-1] if '_' in dag_id else ''
    if not instance or not _INSTANCE_PATTERN.match(instance):
        raise ValueError(
            f"Could not derive a safe instance from dag_id={dag_id!r}"
        )
    return instance


def _customer_id_from_conf():
    """Read the per-tenant customerId from dag_run conf. Required."""
    ctx = rail.get_current_context()
    conf = (ctx.get('dag_run').conf if ctx.get('dag_run') else None) or {}
    customer_id = conf.get('customerId')
    if not customer_id or not _INSTANCE_PATTERN.match(str(customer_id)):
        raise ValueError(
            "dag_run.conf.customerId is missing or contains unsafe "
            "characters; cannot derive firm map Variable key"
        )
    return str(customer_id)


def _query_map_firm_row(firm_id):
    """Look up a customer row in map_firm by FirmID (IsVendor='N').

    Returns the row dict {FirmID, QBOID, IsVendor, Name} or None on miss.
    """
    if not firm_id:
        return None
    query = (
        f"SELECT FirmID, QBOID, IsVendor, Name FROM {map_firm_table_name} "
        f"WHERE FirmID = ? AND IsVendor = 'N' LIMIT 1"
    )
    raw = collection_single_row(query, [str(firm_id)], read_task_id='_lookup_map_firm_by_firm_id')
    if raw is None or isinstance(raw, dict):
        return raw
    # Defensive tuple guard
    return dict(zip(['FirmID', 'QBOID', 'IsVendor', 'Name'], raw))


# ---------------------------------------------------------------------------
# QBO realm capability flags
#
# QBO rejects fields that require a feature to be enabled on the target
# realm (e.g. CurrencyRef when Multi-Currency is off; Taxable when sales
# tax isn't configured). Detecting these at runtime via QBO Preferences
# would cost an extra API call per record, so instead we read a
# per-tenant Variable maintained by ops. Conservative default is False
# (don't send the gated field) so onboarding a new tenant never crashes
# on day one.
#
# Lookup order (first match wins):
#   1. qbo_{capability}_enabled_{customer_id}_{instance}   # per-tenant
#   2. qbo_{capability}_enabled_{instance}                 # per-instance
#   3. False
# ---------------------------------------------------------------------------

def _qbo_capability_enabled(capability):
    """Return True iff the named QBO capability is enabled for this tenant.

    Capability names are short snake_case strings; common values:
      - 'multi_currency'   (gates CurrencyRef)
      - 'sales_tax'        (gates Taxable, TaxExemption*)
      - 'time_tracking'    (gates TimeActivity BillableStatus)
    """
    try:
        instance = _instance_from_dag_id()
    except ValueError:
        instance = None
    try:
        customer_id = _customer_id_from_conf()
    except ValueError:
        customer_id = None
    for key in (
        f'qbo_{capability}_enabled_{customer_id}_{instance}'
        if instance and customer_id else None,
        f'qbo_{capability}_enabled_{instance}' if instance else None,
    ):
        if not key:
            continue
        raw = Variable.get(key, default_var=None)
        if raw is None:
            continue
        return str(raw).lower() in ('1', 'true', 'yes', 'on')
    return False


# ---------------------------------------------------------------------------
# Router helpers
# ---------------------------------------------------------------------------

def lookup_customer_by_firm_id():
    """Find an existing firm row in the shared map_firm collection by FirmID.

    Returns the row dict {FirmID, QBOID, IsVendor, Name} or None.
    """
    conf = rail.get_current_context()['dag_run'].conf
    firm_id = conf.get('ClientID')
    return _query_map_firm_row(firm_id)


def check_customer_exists_in_lookup():
    """IfOperator test: did get_customer_from_lookup return a row?"""
    row = rail.result('get_customer_from_lookup')
    return bool(row and row.get('QBOID'))


def build_customer_conf(operation_type):
    """Build conf for the customer_create / customer_update child DAG.

    Forwards the firm dict, connections, customerId, and (for update) the
    mapped QBO Customer Id from the firm map row.
    """
    conf = rail.get_current_context()['dag_run'].conf
    result = {
        **conf,
        'type': operation_type,
        'connections': conf.get('connections'),
    }
    if operation_type == 'update':
        firm_row = rail.result('get_customer_from_lookup') or {}
        result['qbo_customer_id'] = firm_row.get('QBOID')
    return result


def collect_triggered_dagrun_ids():
    """Collect dag run(s) from whichever trigger executed (create or update).

    Only one of the two trigger tasks actually runs per router execution
    (the other is skipped by the IfOperator), so its XCom is absent. We
    use rail.has_result (or fall back to an AirflowSkipException catch)
    to distinguish "skipped/not run" from a real failure that should
    propagate.
    """
    dag_runs = []
    for task_id in ('trigger_customer_create', 'trigger_customer_update'):
        try:
            result = rail.result(task_id)
        except KeyError:
            # Sibling branch was skipped by the IfOperator; no XCom.
            continue
        if result is not None:
            dag_runs.append(result)
    return dag_runs


# ---------------------------------------------------------------------------
# VP response normalization
# ---------------------------------------------------------------------------

def _unwrap_list_response(raw, list_keys):
    """Normalize a VP GET response (bare list or wrapped dict) to a list."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in list_keys:
            value = raw.get(key)
            if isinstance(value, list):
                return value
    return []


def _firm_record_from_response(task_id):
    """Generic firm-record extractor for any task whose XCom holds the firm
    response (list or dict). Missing XCom (skipped upstream) returns {}.
    """
    try:
        response = rail.result(task_id)
    except KeyError:
        return {}
    if isinstance(response, list) and response:
        return response[0] or {}
    if isinstance(response, dict):
        return response
    return {}


# ---------------------------------------------------------------------------
# VP -> QBO transform helpers
# ---------------------------------------------------------------------------

_BILLING_ADDRESS_TYPE = 'Billing'
_SHIPPING_ADDRESS_TYPE = 'Shipping'


def _filter_none(body):
    """Drop None values so QBO doesn't reject empty fields."""
    return {k: v for k, v in body.items() if v is not None}


def _vp_status_to_qbo_active(status):
    """VP Status (A/I) -> QBO Active (bool). None passes through."""
    if status == 'A':
        return True
    if status == 'I':
        return False
    return None


def _find_address_by_type(addresses, address_type):
    """Find an address dict by Address type label (case-insensitive)."""
    target = address_type.upper()
    for addr in addresses:
        if not isinstance(addr, dict):
            continue
        if (addr.get('Address') or '').upper() == target:
            return addr
    return None


def _qbo_addr_from_vp(vp_addr):
    """Convert a VP address dict to QBO address shape (BillAddr/ShipAddr)."""
    if not vp_addr:
        return None
    body = {
        'Line1': vp_addr.get('Address1'),
        'Line2': vp_addr.get('Address2'),
        'Line3': vp_addr.get('Address3'),
        'City': vp_addr.get('City'),
        'CountrySubDivisionCode': vp_addr.get('State'),
        'PostalCode': vp_addr.get('Zip'),
        'Country': vp_addr.get('Country'),
    }
    cleaned = _filter_none(body)
    return cleaned or None


def _resolve_billing_and_shipping_addresses():
    """Return (billing_addr_dict, shipping_addr_dict).

    Returns each independently — `None` for shipping when no Shipping-type
    record exists. The body builders use this distinction:
      - On create: omit ShipAddr if shipping is None.
      - On update: leave the existing QBO ShipAddr untouched if shipping
        is None (do NOT overwrite with billing — that would clobber a
        valid shipping address on the QBO side).
    """
    addresses = _unwrap_list_response(
        rail.result('get_firm_addresses_from_vp'),
        ('array', 'Body', 'body', 'addresses', 'CLAddress'),
    )
    billing = _find_address_by_type(addresses, _BILLING_ADDRESS_TYPE)
    shipping = _find_address_by_type(addresses, _SHIPPING_ADDRESS_TYPE)
    return billing, shipping


def _resolve_primary_contact():
    """Pick the primary contact dict from get_firm_contact_from_vp.

    VP returns a list of contacts on the firm. Prefer the row flagged
    QBOIsMainContact='true'; otherwise return the first row.
    """
    contacts = _unwrap_list_response(
        rail.result('get_firm_contact_from_vp'),
        ('array', 'Body', 'body', 'rows', 'Contact', 'contacts'),
    )
    for c in contacts:
        if isinstance(c, dict) and str(c.get('QBOIsMainContact', '')).lower() == 'true':
            return c
    return contacts[0] if contacts and isinstance(contacts[0], dict) else {}


def build_firm_contacts_filter():
    """Filter string for GET /contact to list contacts attached to this firm."""
    conf = rail.get_current_context()['dag_run'].conf
    client_id = conf.get('ClientID')
    return (
        f'?filterHash[0][name]=ClientID&'
        f'filterHash[0][value]={client_id}'
    )


def _qbo_web_addr(raw):
    """Normalise a VP firm WebSite value into a QBO WebAddr.

    QBO's URL validator rejects bare hostnames ('www.foo.com'); the URI
    MUST start with a scheme. We prepend 'https://' if missing. Returns
    None for empty/whitespace input so _filter_none drops the field.
    """
    if not raw:
        return None
    value = str(raw).strip()
    if not value:
        return None
    if '://' not in value:
        value = 'https://' + value
    return {'URI': value}


def _qbo_display_name(firm_name, firm_client_id):
    """Build a unique-per-realm QBO DisplayName.

    QBO requires Customer.DisplayName to be unique within a realm.
    Two VP firms sharing the same Name would collide on create with
    code=6240 ("Duplicate Name Exists Error"). We suffix the VP
    ClientID (8-char prefix) when the firm has a ClientID so the
    DisplayName is deterministic per source row, while CompanyName
    stays the raw Name for human readability.

    If two firms truly share both Name AND the first 8 chars of
    ClientID (extremely unlikely — ClientIDs are 32-char hex/uuid),
    a second-pass collision-detect-and-suffix would be needed; not
    implemented because the probability is negligible.
    """
    if not firm_name:
        return None
    suffix = (firm_client_id or '')[:8]
    if not suffix:
        return firm_name
    return f"{firm_name} ({suffix})"


def _build_customer_body_core():
    """Build the shared subset of QBO Customer fields used by create and
    update. Address/contact/currency derivation is centralised here so the
    two body builders stay in sync.
    """
    firm = _firm_record_from_response('get_firm_from_vp')
    billing_addr, shipping_addr = _resolve_billing_and_shipping_addresses()
    contact = _resolve_primary_contact()

    name = firm.get('Name')
    display_name = _qbo_display_name(name, firm.get('ClientID'))
    active = _vp_status_to_qbo_active(firm.get('Status'))

    primary_phone = (billing_addr or {}).get('Phone') or contact.get('Phone')
    primary_email = (billing_addr or {}).get('Email') or contact.get('Email')

    body = {
        'DisplayName': display_name,
        'CompanyName': name,
        'Active': active,
        'BillAddr': _qbo_addr_from_vp(billing_addr),
        'ShipAddr': _qbo_addr_from_vp(shipping_addr),
        'PrimaryPhone': (
            {'FreeFormNumber': primary_phone} if primary_phone else None
        ),
        'PrimaryEmailAddr': (
            {'Address': primary_email} if primary_email else None
        ),
        'GivenName': contact.get('FirstName'),
        'FamilyName': contact.get('LastName'),
        'WebAddr': _qbo_web_addr(firm.get('WebSite')),
    }
    # Feature-gated fields. See _qbo_capability_enabled for the
    # per-tenant flag. Conservative defaults — when in doubt, don't send.
    if _qbo_capability_enabled('sales_tax'):
        body['Taxable'] = True
    currency_code = firm.get('CustomCurrencyCode')
    if currency_code and _qbo_capability_enabled('multi_currency'):
        body['CurrencyRef'] = {'value': currency_code}
    return _filter_none(body)


def build_create_customer_body():
    """POST body for QBO /customer (create new customer)."""
    return _build_customer_body_core()


# ---------------------------------------------------------------------------
# Recovery path: search QBO by CompanyName BEFORE creating, so a firm
# whose map row is missing (manually-deleted Variable, fresh migration,
# disaster recovery) doesn't try to re-create a customer that QBO already
# has and trigger a `Duplicate Name Exists` 6240. The Workato recipe did
# this same search-first dance.
# ---------------------------------------------------------------------------

_SQL_QUOTE_RE = re.compile(r"['\\]")


def _sql_escape(value):
    r"""Conservative SQL string escape for QBO's pseudo-SQL.

    QBO accepts `\\'` and `\\\\` as escapes inside single-quoted literals.
    """
    return _SQL_QUOTE_RE.sub(lambda m: '\\' + m.group(0), value)


def build_qbo_search_by_company_name_query(**_context):
    """QBO SQL: find any Customer whose CompanyName equals the VP firm Name.

    Used to recover from a missing firm-map row when QBO already has the
    customer (e.g. the map was reset, or this is a re-onboarded tenant).
    Returns a query that matches zero rows if the firm Name is empty.
    """
    firm = _firm_record_from_response('get_firm_from_vp')
    name = firm.get('Name')
    if not name:
        return "select * from Customer where CompanyName = '__missing__'"
    escaped = _sql_escape(str(name))
    return f"select * from Customer where CompanyName = '{escaped}'"


def _qbo_search_response_record(task_id):
    """First Customer row from a QuickBooksCustomerOperator search XCom."""
    try:
        response = rail.result(task_id)
    except KeyError:
        return {}
    if not isinstance(response, dict) or not response.get('success'):
        return {}
    data = response.get('data')
    if isinstance(data, list) and data:
        return data[0] or {}
    if isinstance(data, dict):
        return data
    return {}


def qbo_customer_already_exists():
    """IfOperator test: did the by-CompanyName search find an existing
    Customer in QBO? If yes, route the create-dag into the recovery
    branch instead of attempting a new POST that would 6240 on
    Duplicate Name.
    """
    return bool(
        _qbo_search_response_record(
            'search_qbo_customer_by_name'
        ).get('Id')
    )


def capture_existing_qbo_customer_id_from_recovery():
    """Use the existing QBO Customer Id discovered by the search.
    Acts as the source of truth for downstream firm-map write +
    VP write-back when we recover an unmapped firm.
    """
    record = _qbo_search_response_record('search_qbo_customer_by_name')
    return record.get('Id')


_QBO_UPDATE_PRESERVE_FIELDS = frozenset({
    # Required for the PUT itself.
    'Id', 'SyncToken',
    # Customer-shape fields that we want to retain if the existing
    # record had them and our overlay doesn't override. Anything
    # outside this set is dropped from the merged body to avoid sending
    # QBO read-only or runtime-only fields (Balance, BalanceWithJobs,
    # MetaData, domain, sparse, etc.) which QBO either rejects or
    # silently misinterprets.
    'DisplayName', 'CompanyName', 'GivenName', 'FamilyName',
    'MiddleName', 'Title', 'Suffix',
    'PrintOnCheckName', 'Notes', 'Job', 'BillWithParent', 'ParentRef',
    'Level', 'PreferredDeliveryMethod', 'ResaleNum',
    'BillAddr', 'ShipAddr', 'PrimaryPhone', 'AlternatePhone',
    'Mobile', 'Fax', 'PrimaryEmailAddr', 'WebAddr',
    'Active', 'Taxable', 'TaxExemptionReasonId',
    'DefaultTaxCodeRef', 'SalesTermRef', 'PaymentMethodRef',
})


def build_update_customer_body(**_context):
    """POST body for QBO /customer update.

    QBO update is a full PUT. Start from a *curated subset* of the
    existing QBO record so untouched user-editable fields are preserved,
    then overlay the VP-derived fields on top. We deliberately strip
    everything outside the allowlist (Balance, MetaData, domain, sparse,
    etc.) — those are read-only or runtime-only and forwarding them
    either causes QBO to reject the PUT or silently overwrite state.
    """
    existing = _existing_qbo_customer_record()
    preserved = {
        k: existing[k]
        for k in _QBO_UPDATE_PRESERVE_FIELDS
        if k in existing
    }
    overlay = _build_customer_body_core()
    merged = {**preserved, **overlay}
    merged['Id'] = existing.get('Id')
    merged['SyncToken'] = existing.get('SyncToken')
    return _filter_none(merged)


def _existing_qbo_customer_record():
    """Pull the first Customer row from search_existing_qbo_customer XCom."""
    response = rail.result('search_existing_qbo_customer') or {}
    data = response.get('data') if isinstance(response, dict) else None
    if isinstance(data, list) and data:
        return data[0] or {}
    if isinstance(data, dict):
        return data
    return {}


_QBO_ID_PATTERN = re.compile(r'^[0-9]+$')


def build_update_customer_search_query(**_context):
    """QBO SQL query to fetch the existing Customer by mapped Id.

    Reads conf.qbo_customer_id (set by router from the firm map row).
    QBO Customer Ids are unsigned integer strings; validate that shape
    so a tampered firm-map row cannot break out of the quoted literal
    and inject arbitrary QB SQL.
    """
    conf = rail.get_current_context()['dag_run'].conf
    qbo_customer_id = conf.get('qbo_customer_id')
    if not qbo_customer_id or not _QBO_ID_PATTERN.match(str(qbo_customer_id)):
        # Missing or unsafe mapping. Force an empty result set so the
        # downstream IfOperator routes into the fallback-create branch.
        return "select * from Customer where Id = '__missing__'"
    return f"select * from Customer where Id = '{qbo_customer_id}'"


def has_existing_qbo_customer():
    """IfOperator test: did the QBO search succeed AND return a row?

    Distinguishes three outcomes:
      - success + at least one row -> True  (update path)
      - success + zero rows         -> False (fallback-create path)
      - failure (5xx, timeout, etc) -> raises, so update_dag fails and
        the error surfaces in catch_customer_dag_error. We deliberately
        do NOT silently route a search failure into fallback-create:
        that would create a duplicate QBO Customer and overwrite the
        firm map to point at the duplicate.
    """
    response = rail.result('search_existing_qbo_customer')
    if not isinstance(response, dict):
        raise RuntimeError(
            "Unexpected response shape from search_existing_qbo_customer; "
            f"got {type(response).__name__}"
        )
    if not response.get('success'):
        error = response.get('error') or 'unknown error'
        raise RuntimeError(
            f"QBO customer search failed; refusing to fall back to "
            f"create to avoid duplicate creation. Underlying error: "
            f"{error}"
        )
    return bool(_existing_qbo_customer_record().get('Id'))


# ---------------------------------------------------------------------------
# VP write-back body (recipe step 9: store QBOCustomerID on the VP firm)
# ---------------------------------------------------------------------------

def build_vp_firm_writeback_body():
    """PUT body for /firm/{ClientID} that records the new QBO Customer Id
    on the VP firm.

    Reads the Id from whichever capture branch ran (new-create vs
    recovered-existing). VP's firm schema exposes the cross-system id
    as `QBOID` — same field name used in the shared map_firm S3 table.
    """
    qbo_id = None
    for task_id in (
        'capture_qbo_customer_id',
        'capture_existing_qbo_customer_id',
    ):
        try:
            value = rail.result(task_id)
        except KeyError:
            continue
        if value:
            qbo_id = value
            break
    return {'QBOID': qbo_id}


# ---------------------------------------------------------------------------
# QBO create response handling + firm map writes
# ---------------------------------------------------------------------------

def _qbo_customer_id_from(task_id):
    """Pull Customer.Id out of a QuickBooksCustomerOperator create response."""
    response = rail.result(task_id) or {}
    if not isinstance(response, dict):
        return None
    if not response.get('success'):
        return None
    data = response.get('data')
    if isinstance(data, list) and data:
        return (data[0] or {}).get('Id')
    if isinstance(data, dict):
        return data.get('Id')
    return None


def capture_qbo_customer_id_from_create():
    """Return the Id of the customer just created in QBO."""
    return _qbo_customer_id_from('create_customer_in_qbo')


def capture_qbo_customer_id_from_create_fallback():
    """Return the Id from the fallback-create POST (update DAG)."""
    return _qbo_customer_id_from('create_customer_in_qbo_fallback')


def _write_firm_map_row(firm_id, qbo_customer_id, name):
    """Upsert a customer row (IsVendor='N') in the shared map_firm collection.

    Uses S3UpsertCollectionOperator keyed on (QBOID, IsVendor) — the UNIQUE
    index on map_firm. Single S3 cycle (atomic).
    """
    if not firm_id or not qbo_customer_id:
        logger.warning(
            "Skipping firm map write — missing key field "
            "(firm_id=%s, qbo_customer_id=%s)", firm_id, qbo_customer_id,
        )
        return None

    context = rail.get_current_context()
    _, customer, _ = collection_integration(context)
    try:
        collection_upsert(
            map_firm_table_name,
            key_columns=['QBOID', 'IsVendor'],
            data_columns={
                'FirmID': str(firm_id), 'QBOID': str(qbo_customer_id),
                'IsVendor': 'N', 'Name': name,
            },
            context=context,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if isinstance(exc, FileNotFoundError) or 'no such table' in str(exc).lower():
            raise RuntimeError(
                f"map_firm write failed for customer '{customer}': "
                f"the mapping_sync collection / '{map_firm_table_name}' table is "
                f"missing. The firm already exists in QBO "
                f"(FirmID={firm_id}, QBOID={qbo_customer_id}) but its mapping "
                f"could not be persisted — run mapping_sync for this customer "
                f"first, then re-sync."
            ) from exc
        raise

    logger.info(
        "Wrote map_firm entry: FirmID %s -> QBOID %s", firm_id, qbo_customer_id,
    )
    return qbo_customer_id


def add_customer_to_firm_map():
    """Insert/update firm map row after a successful create.

    Reads the QBO Customer Id from whichever branch ran in the create
    DAG: either `capture_qbo_customer_id` (we POSTed a new customer)
    or `capture_existing_qbo_customer_id` (the search-recovery branch
    found one already in QBO). Whichever task was skipped raises
    KeyError on rail.result; we fall through.
    """
    conf = rail.get_current_context()['dag_run'].conf
    qbo_id = None
    for task_id in (
        'capture_qbo_customer_id',
        'capture_existing_qbo_customer_id',
    ):
        try:
            value = rail.result(task_id)
        except KeyError:
            continue
        if value:
            qbo_id = value
            break
    return _write_firm_map_row(
        firm_id=conf.get('ClientID'),
        qbo_customer_id=qbo_id,
        name=conf.get('Name'),
    )


def add_customer_to_firm_map_fallback():
    """Patch firm map row after fallback-create (update DAG)."""
    conf = rail.get_current_context()['dag_run'].conf
    return _write_firm_map_row(
        firm_id=conf.get('ClientID'),
        qbo_customer_id=rail.result('capture_fallback_customer_id'),
        name=conf.get('Name'),
    )


def refresh_firm_map_row():
    """Refresh the firm map row after a successful update.

    Wired to run only on the success branch of the update DAG (no
    `trigger_rule='none_failed'`) so it cannot be reached if any upstream
    task failed — that keeps the firm map from being touched after a
    half-applied update.

    Re-writes the row with the current Name from VP so a rename
    propagates to the map. QBOCustomerID comes from conf (the value the
    router routed us with), with the existing row as a backup if conf is
    missing it.
    """
    conf = rail.get_current_context()['dag_run'].conf
    firm_id = conf.get('ClientID')
    if not firm_id:
        return None
    qbo_customer_id = conf.get('qbo_customer_id')
    existing = _query_map_firm_row(str(firm_id)) or {}
    if not qbo_customer_id:
        qbo_customer_id = existing.get('QBOID')
    firm = _firm_record_from_response('get_firm_from_vp')
    name = firm.get('Name') or conf.get('Name') or existing.get('Name')
    return _write_firm_map_row(firm_id, qbo_customer_id, name)


# ---------------------------------------------------------------------------
# Error capture (return dict; do NOT raise — keeps DAG SUCCESS so parent
# WaitForDagRunsSensor never sees a failed run)
# ---------------------------------------------------------------------------

def _format_firm_label(firm_id, firm_name):
    """Format the firm identifier prefix for error messages."""
    if firm_name and str(firm_name).strip():
        return f"Firm {firm_id} ({str(firm_name).strip()})"
    return f"Firm {firm_id}"


def capture_create_error(firm_id, firm_name, error_message):
    return {
        'error': (
            f"{_format_firm_label(firm_id, firm_name)} - "
            f"create failed: {error_message}"
        )
    }


def capture_update_error(firm_id, firm_name, error_message):
    return {
        'error': (
            f"{_format_firm_label(firm_id, firm_name)} - "
            f"update failed: {error_message}"
        )
    }


def capture_router_dag_error(firm_id, firm_name, fallback_error_message):
    """Aggregate child errors; fall back to local message; return dict or None.

    Runs on `trigger_rule='all_done'`, so it executes for every router run
    (green or red). Every branch is wrapped in defensive try/except: an
    uncaught exception here would mark the entire green run red, masking
    real errors in the success aggregation.
    """
    child_errors = []
    try:
        gathered = rail.result('gather_customer_dag_errors')
        if gathered:
            child_errors = (
                gathered if isinstance(gathered, list) else [gathered]
            )
    except KeyError:
        # The gather task was skipped (upstream branch never produced
        # rows). Treat as no child errors.
        pass
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # Anything else: log and fall back to the local message so a
        # bug in gather doesn't poison the entire green run.
        print(
            f"capture_router_dag_error: failed to read "
            f"gather_customer_dag_errors XCom: {exc!r}"
        )

    try:
        if child_errors:
            error_message = ' | '.join(
                (e or {}).get('error', str(e)) for e in child_errors
            )
        elif fallback_error_message:
            error_message = (
                f"{_format_firm_label(firm_id, firm_name)} - "
                f"sync failed: {fallback_error_message}"
            )
        else:
            return None
        return {'error': error_message}
    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(f"capture_router_dag_error: formatting failed: {exc!r}")
        return {
            'error': (
                f"{_format_firm_label(firm_id, firm_name)} - "
                f"sync failed (error formatting bug); see Airflow logs"
            )
        }

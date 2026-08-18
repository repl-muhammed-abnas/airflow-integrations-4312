"""
Common utility methods for QBO -> VP Customer Sync.

Translates the Workato recipes (single poll filtered by LastUpdatedTime
in the dispatcher, shared customer/vendor processor, firm_qboid_exists,
upsert_firm_address, contact_to_vp, dvp_insert_update_veaccounting) into
Python callables for the 4-DAG Airflow template
(main -> dispatcher -> router -> create | update). Error reporting goes
to middleware via PostDagRunDetailsToMiddlewareApiOperator + FailOperator
on the dispatcher's failure branch; no email/CSV path here (matches
vendor_sync).
"""
# pylint: disable=invalid-name,broad-exception-caught,too-many-locals
import logging
import re
from datetime import datetime, timezone
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
import rail
from vp_quickbooks_integration.customer_sync.config import (
    initial_sync_time,
)
from vp_quickbooks_integration.common.python_callable_method import (
    collection_integration,
    collection_rows,
    collection_upsert,
    watermark_key_template,
)
from vp_quickbooks_integration.common.tables import (
    MAP_FIRM_COLUMNS,
    MAP_FIRM_TABLE_NAME as map_firm_table_name,
)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Watermark helper. Single Variable per customerId, naming
# matches vendor_sync's pattern. One QBO query filtered by
# `MetaData.LastUpdatedTime` captures BOTH new and updated customers in a
# single stream — QBO bumps LastUpdatedTime on create, so the
# Workato-era split into separate `new_customer` / `updated_customer`
# triggers wasn't a domain requirement, just a Workato platform artifact.
# ---------------------------------------------------------------------------
WATERMARK_KEY_TEMPLATE = watermark_key_template('customer_sync')


def _customer_id_from_conf():
    return rail.get_current_context()['dag_run'].conf.get('customerId')


def _watermark_key(template, customer_id, instance):
    return template.format(
        customer_id=customer_id or 'default', instance=instance
    )


def _now_iso():
    """
    UTC ISO-8601 with milliseconds and 'Z' suffix — matches `initial_sync_time`
    in config.py so first-run and steady-state watermarks share one shape when
    inlined into QBO SQL `>= '...'` predicates.
    """
    now = datetime.now(timezone.utc)
    return now.strftime('%Y-%m-%dT%H:%M:%S') + f'.{now.microsecond // 1000:03d}Z'


# Format guard for any timestamp that gets f-stringed into QBO SQL. The
# watermark value comes from an Airflow Variable we set ourselves, so the
# risk surface is internal corruption / accidental manual edit rather than
# user-supplied input — but inlining anything into SQL without validation
# is a habit worth not picking up. Pattern matches both the millisecond
# (config.initial_sync_time) and second-precision shapes plus an optional
# `Z` or offset suffix.
_QBO_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:?\d{2})?$"
)


def _validate_qbo_timestamp(value, field_name):
    if not isinstance(value, str) or not _QBO_TIMESTAMP_RE.match(value):
        raise ValueError(
            f"Refusing to build QBO query: {field_name}={value!r} is not a "
            "valid ISO-8601 timestamp. Watermark Variable may have been "
            "corrupted; reset it before retrying."
        )
    return value


def prepare_customer_sync_timestamps_method(instance):
    """
    Capture the half-open sync window `[last_sync_time, current_sync_time)`.
    Mirrors vendor_sync's `prepare_sync_timestamps`.

    `current_sync_time` is captured at the start of the run and written
    back to the watermark Variable at the end, so customers modified
    mid-run are picked up exactly once by the next run.
    """
    customer_id = _customer_id_from_conf()
    key = _watermark_key(
        WATERMARK_KEY_TEMPLATE, customer_id, instance
    )
    current = _now_iso()

    try:
        last_sync_time = Variable.get(key)
    except KeyError:
        last_sync_time = initial_sync_time
        logger.info(
            "Variable '%s' not found; using initial_sync_time", key
        )

    logger.info(
        "Customer sync window: [%s, %s)", last_sync_time, current
    )
    return {
        'last_sync_time': last_sync_time,
        'current_sync_time': current,
    }


def update_customer_last_sync_times_method(instance):
    """Persist `current_sync_time` into the watermark Variable.

    Skips the advance when the integration is disabled for this tenant
    (so re-enabling later catches up on the disabled window). The
    dispatcher uses `trigger_rule='all_done'` on this task to keep the
    watermark advancing even when the error branch's FailOperator
    raises; the disabled-flag check below is what preserves the
    no-advance-when-disabled behaviour.
    """
    try:
        is_enabled = rail.result('check_disabled_flag')
    except KeyError:
        is_enabled = True
    if not is_enabled:
        logger.info("Integration disabled; skipping watermark advance")
        return None
    customer_id = _customer_id_from_conf()
    key = _watermark_key(
        WATERMARK_KEY_TEMPLATE, customer_id, instance
    )
    timestamps = rail.result('prepare_sync_timestamps')
    current = timestamps['current_sync_time']
    Variable.set(key, current)
    logger.info("Advanced customer watermark '%s' to: %s", key, current)
    return current


# ---------------------------------------------------------------------------
# Disabled-flag check. Resolves per-(instance, customerId) first, then falls
# back to instance-level for backward-compatibility / ops kill-switch.
# Replaces Workato 014_503_PSA.CFG_DisableCustomerIntegration account property
# (which was per-tenant in the source recipe).
# ---------------------------------------------------------------------------
def is_integration_enabled_method(instance):
    """
    True unless either disabled flag is set to 'true'.

    Lookup order:
      1. CFG_DisableCustomerIntegration_{instance}_{customer_id}  (per-tenant)
      2. CFG_DisableCustomerIntegration_{instance}                (per-instance kill switch)
    """
    customer_id = _customer_id_from_conf() or 'default'
    tenant_key = f'CFG_DisableCustomerIntegration_{customer_id}_{instance}'
    instance_key = f'CFG_DisableCustomerIntegration_{instance}'

    tenant_flag = Variable.get(tenant_key, default_var=None)
    if tenant_flag is not None and str(tenant_flag).strip().lower() == 'true':
        logger.info(
            "Customer integration disabled for tenant '%s' on instance '%s' "
            "via %s", customer_id, instance, tenant_key
        )
        return False

    instance_flag = Variable.get(instance_key, default_var='false')
    if str(instance_flag).strip().lower() == 'true':
        logger.info(
            "Customer integration disabled for entire instance '%s' via %s",
            instance, instance_key
        )
        return False

    return True


# ---------------------------------------------------------------------------
# Router DAG helpers (mirrors vendor_sync's router pattern).
# The router DAG decides create vs update purely from firm-map state.
# ---------------------------------------------------------------------------
def lookup_firm_by_qboid(instance):
    """Find an existing firm row in the firm map by QBOID.

    Returns the row dict ``{FirmID, QBOID, IsVendor, Name}`` or ``None``.
    Mirrors vendor_sync's lookup_firm_by_qboid. `instance` is passed
    explicitly because the customer-sync firm map is per-instance
    (shared with timesheets_sync via `psa_vp_qbo_firm_map_{instance}`),
    unlike vendor_sync's globally-keyed map.
    """
    conf = rail.get_current_context()['dag_run'].conf or {}
    qbo_id = str(conf.get('QBOID') or '').strip()
    if not qbo_id:
        return None
    for row in get_firm_mapping_method(instance):
        if str(row.get('QBOID') or '').strip() == qbo_id:
            return row
    return None


def check_firm_exists_in_lookup():
    """IfOperator test: did get_firm_from_lookup return a row?"""
    return rail.result('get_firm_from_lookup') is not None


def build_customer_conf(operation_type):
    """Build conf for the customer_create / customer_update child DAG.

    Mirrors vendor_sync.build_vendor_conf. For the update branch we forward
    the existing FirmID from the firm-map lookup so the update leaf doesn't
    need to repeat the lookup.
    """
    conf = rail.get_current_context()['dag_run'].conf or {}
    result = {
        **conf,
        'type': operation_type,
        'connections': conf.get('connections'),
    }
    if operation_type == 'update':
        firm_row = rail.result('get_firm_from_lookup') or {}
        if firm_row:
            result['vp_client_id'] = firm_row.get('FirmID')
    return result


def collect_triggered_dagrun_ids():
    """Collect dag run(s) from whichever trigger executed (create or update)."""
    dag_runs = []
    for task_id in ('trigger_customer_create', 'trigger_customer_update'):
        try:
            result = rail.result(task_id)
            if result is not None:
                dag_runs.append(result)
        except Exception:
            logger.debug(
                "rail.result('%s') unavailable in collect_triggered_dagrun_ids",
                task_id, exc_info=True
            )
    return dag_runs


def capture_router_dag_error(qboid, display_name, fallback_error):
    """Aggregate child errors from the create/update grandchild; return None on clean run."""
    child_errors = []
    try:
        gathered = rail.result('gather_customer_dag_errors')
        if gathered:
            child_errors.extend(
                e for e in gathered if e and e.get('error')
            )
    except Exception:
        logger.debug(
            "No gather_customer_dag_errors XCom available; child DAG "
            "may not have triggered", exc_info=True
        )

    if child_errors:
        message = ' | '.join(
            e.get('error', str(e)) for e in child_errors if e
        )
    elif fallback_error:
        message = (
            f"QBOID {qboid} ({display_name}) - customer router failed: "
            f"{fallback_error}"
        )
    else:
        return None

    return {
        'error': message,
        'QBOID': qboid,
        'DisplayName': display_name,
    }


# ---------------------------------------------------------------------------
# Payload extraction. Workato triggers flatten the QBO Customer JSON to ~25
# fields before invoking the shared processor; we do the same so the
# router DAG receives a stable dict regardless of poll type.
# ---------------------------------------------------------------------------
def _addr_block(raw_addr):
    raw_addr = raw_addr or {}
    return {
        'Line1': raw_addr.get('Line1') or '',
        'Line2': raw_addr.get('Line2') or '',
        'Line3': raw_addr.get('Line3') or '',
        'City': raw_addr.get('City') or '',
        'State': raw_addr.get('CountrySubDivisionCode') or '',
        'Zip': raw_addr.get('PostalCode') or '',
        'Country': raw_addr.get('Country') or '',
    }


def _extract_one(raw):
    """Flatten a single raw QBO Customer object into the recipe's payload shape.

    The create-vs-update decision lives in the ROUTER DAG (firm-map
    lookup), not here. We just emit the latest snapshot.
    """
    return {
        'QBOID': str(raw.get('Id') or ''),
        'DisplayName': raw.get('DisplayName') or '',
        'Company': raw.get('CompanyName') or '',
        'FirstName': raw.get('GivenName') or '',
        'MiddleName': raw.get('MiddleName') or '',
        'LastName': raw.get('FamilyName') or '',
        'Suffix': raw.get('Suffix') or '',
        'Title': raw.get('Title') or '',
        'Email': (raw.get('PrimaryEmailAddr') or {}).get('Address') or '',
        'Website': (raw.get('WebAddr') or {}).get('URI') or '',
        'Phone': (raw.get('PrimaryPhone') or {}).get('FreeFormNumber') or '',
        'Mobile': (raw.get('Mobile') or {}).get('FreeFormNumber') or '',
        'Fax': (raw.get('Fax') or {}).get('FreeFormNumber') or '',
        'Billing': _addr_block(raw.get('BillAddr')),
        'Shipping': _addr_block(raw.get('ShipAddr')),
        # IsVendor / Form1099 are hardcoded 'N' here because this DAG only
        # processes the QBO *Customer* entity (the matching recipe never set
        # them to 'Y'). The shared map_firm S3 table is also written by
        # vendor_sync (IsVendor='Y'). Don't change these defaults without
        # first checking the vendor-sync contract — a row flipped from 'N'
        # to 'Y' in this customer path would mis-classify a customer as a
        # vendor in timesheets_sync's `resolve_firm_method`.
        'IsVendor': 'N',
        'Form1099': 'N',
        'ActiveStatus': bool(raw.get('Active', True)),
        'SyncToken': raw.get('SyncToken') or '',
        'CreateTime': (raw.get('MetaData') or {}).get('CreateTime') or '',
        'LastUpdatedTime': (
            (raw.get('MetaData') or {}).get('LastUpdatedTime') or ''
        ),
    }


def extract_customer_list_method():
    """
    Extract the customer list from the QuickBooksCustomerOperator response
    and flatten each entity into the shape downstream DAGs consume.

    Mirrors vendor_sync's `extract_vendor_list`. A single poll (filtered
    by `MetaData.LastUpdatedTime`) captures both new and updated customers
    in QBO, so no merge/dedupe is required.
    """
    result = rail.result('get_recently_changed_customers')
    if isinstance(result, dict) and not result.get('success', True):
        logger.warning(
            "QuickBooks customer query failed: %s",
            result.get('error')
        )
        return []
    if isinstance(result, dict):
        raw_list = result.get('data') or result.get('Customer') or []
    elif isinstance(result, list):
        raw_list = result
    else:
        raw_list = []
    customers = [_extract_one(entity) for entity in raw_list if entity]
    logger.info("Found %d recently changed customers", len(customers))
    return customers


# ---------------------------------------------------------------------------
# Firm-mapping table — backed by the shared mapping_sync S3 collection.
# Schema: list[{FirmID, QBOID, IsVendor (Y|N), Name}]
# customer_sync writes IsVendor='N' rows (QBO Customer -> VP Firm direction).
# Shared with vendor_sync (IsVendor='Y'), customer_sync_upsert, timesheets_sync.
# ---------------------------------------------------------------------------

def _load_full_table(table_name):
    return collection_rows(table_name, MAP_FIRM_COLUMNS, '1=1', [])


def _write_map_firm_row(firm_id, qbo_id, is_vendor, name):
    """Upsert a row in the shared map_firm collection.

    Uses S3UpsertCollectionOperator keyed on (QBOID, IsVendor) — the UNIQUE
    index on map_firm — so customer (IsVendor='N') and vendor (IsVendor='Y')
    rows cannot clobber each other. Single S3 cycle (atomic).
    """
    context = rail.get_current_context()
    _, customer, _ = collection_integration(context)
    try:
        return collection_upsert(
            map_firm_table_name,
            key_columns=['QBOID', 'IsVendor'],
            data_columns={
                'FirmID': str(firm_id), 'QBOID': str(qbo_id),
                'IsVendor': is_vendor, 'Name': name,
            },
            context=context,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if isinstance(exc, FileNotFoundError) or 'no such table' in str(exc).lower():
            raise RuntimeError(
                f"map_firm write failed for customer '{customer}': "
                f"the mapping_sync collection / '{map_firm_table_name}' table is "
                f"missing. QBOID={qbo_id}, FirmID={firm_id} — run mapping_sync "
                f"for this customer first, then re-sync."
            ) from exc
        raise


def get_firm_mapping_method(instance):
    """Return list of map_firm rows from the shared mapping_sync S3 collection.

    `instance` is accepted for call-site compatibility but unused —
    the S3 path is per-customer (customerId from dag_run.conf), not per-instance.
    """
    return _load_full_table(map_firm_table_name)


def _payload_from_conf():
    """The flattened QBO customer dict the router passed via dag_run.conf.

    Per vendor_sync's pattern, the dispatcher promotes every record field
    to a top-level conf key (no `record` wrapper). The router forwards
    them unchanged via `build_customer_conf`. So we read directly from
    `dag_run.conf`, not `dag_run.conf['record']`.
    """
    return rail.get_current_context()['dag_run'].conf or {}


def upsert_firm_mapping_method(instance):
    """
    Mirrors the Workato map_firm add/update step (recipe step 6/7).
    Reads the FirmID resolved earlier (or from the just-created firm response)
    and upserts (FirmID, QBOID, IsVendor, Name) into the shared S3 collection.

    `instance` is accepted for call-site compatibility but unused —
    the S3 path is per-customer, not per-instance.
    """
    payload = _payload_from_conf()
    qboid = str(payload.get('QBOID') or '').strip()
    name = payload.get('DisplayName') or ''
    is_vendor = payload.get('IsVendor') or 'N'

    firm_id = _firm_id_for_request()
    if not firm_id:
        logger.warning(
            "Skipping firm-map upsert: no FirmID resolved for QBOID=%s", qboid
        )
        return None

    _write_map_firm_row(firm_id, qboid, is_vendor, name)
    logger.info("Upserted map_firm entry: QBOID=%s -> FirmID=%s", qboid, firm_id)
    return {
        'FirmID': firm_id,
        'QBOID': qboid,
        'IsVendor': is_vendor,
        'Name': name,
    }


# ---------------------------------------------------------------------------
# VP API request body builders.
# ---------------------------------------------------------------------------
def _firm_id_for_request():
    """
    Used by address/contact/accounting body builders, called from BOTH
    the create and update leaves. Two sources, in priority order:
      1. The update leaf carries `dag_run.conf['vp_client_id']` (set by
         the router from the firm-map lookup).
      2. The create leaf reads the ClientID from the `create_firm_in_vp`
         task XCom (which only exists on the create leaf).
    Returns '' if neither is available.
    """
    conf = rail.get_current_context()['dag_run'].conf or {}
    vp_client_id = conf.get('vp_client_id')
    if vp_client_id:
        return str(vp_client_id)
    try:
        created = rail.result('create_firm_in_vp') or {}
    except Exception:
        logger.debug(
            "rail.result('create_firm_in_vp') unavailable; assuming "
            "update leaf without vp_client_id in conf — body builders "
            "will emit ClientID='' and the operator will likely fail.",
            exc_info=True
        )
        created = {}
    # VP POST /firm returns a list (1 element) — unwrap.
    if isinstance(created, list) and created:
        created = created[0] if isinstance(created[0], dict) else {}
    if isinstance(created, dict):
        return (
            created.get('ClientID')
            or created.get('FirmID')
            or (created.get('data') or {}).get('ClientID')
            or ''
        )
    return ''


def build_create_firm_body():
    """
    POST /firm body (Workato `vantagepoint_create_firm` / shared processor
    create branch). Minimum-viable payload — VP fills in defaults.

    VP firm schema uses `Name` and `SortName`; the legacy `LongName` field
    in earlier revisions of this body was rejected by VP with
    `Field LongName does not exist`.
    """
    p = _payload_from_conf()
    name = p.get('DisplayName') or p.get('Company') or ''
    return {
        'Name': name,
        'SortName': name,
        'Status': 'A' if p.get('ActiveStatus', True) else 'I',
        'ClientInd': 'Y',
        'VendorInd': 'N',
    }


def _build_firm_address_body(addr_block, qbo_payload, address_label):
    """Shared shape for billing + shipping POST /firm/{ClientID}/address.

    Field names mirror the known-working vendor_sync analog: FAX (caps),
    Email (not EMailAddress), explicit Address type label with
    PrimaryInd/Payment/Billing flags.
    """
    is_billing = 'Y' if address_label == 'Billing' else 'N'
    # VP workflow requires Country. QBO addresses commonly omit it; default
    # to 'US' (the QBO sandbox tenant is US-based) so the POST clears the
    # workflow rule rather than fails.
    country = addr_block.get('Country') or 'US'
    return {
        'Address': address_label,
        'PrimaryInd': 'true',
        'Payment': 'true' if is_billing == 'Y' else 'false',
        'Billing': is_billing,
        'Address1': addr_block.get('Line1') or '',
        'Address2': addr_block.get('Line2') or '',
        'Address3': addr_block.get('Line3') or '',
        'City': addr_block.get('City') or '',
        'State': addr_block.get('State') or '',
        'Zip': addr_block.get('Zip') or '',
        'Country': country,
        'Phone': qbo_payload.get('Phone') or '',
        'FAX': qbo_payload.get('Fax') or '',
        'Email': qbo_payload.get('Email') or '',
    }


def _address_block_is_empty(addr_block):
    """True when every postal field on the address block is blank.

    VP rejects firm-address POST when Country (and other workflow-required
    fields) are empty, so an all-blank block must skip the POST instead.
    """
    keys = ('Line1', 'Line2', 'Line3', 'City', 'State', 'Zip', 'Country')
    return not any((addr_block or {}).get(k) for k in keys)


def build_billing_address_body():
    p = _payload_from_conf()
    block = p.get('Billing') or {}
    if _address_block_is_empty(block):
        raise AirflowSkipException(
            "No billing address on QBO Customer; skipping POST /firm/address"
        )
    return _build_firm_address_body(block, p, 'Billing')


def build_shipping_address_body():
    p = _payload_from_conf()
    block = p.get('Shipping') or {}
    if _address_block_is_empty(block):
        raise AirflowSkipException(
            "No shipping address on QBO Customer; skipping POST /firm/address"
        )
    return _build_firm_address_body(block, p, 'Shipping')


def build_contact_body():
    """
    POST /contact body. Mirrors `quickbooks_contact_to_vantagepoint` —
    creates/updates the firm contact with the QBO display-name fields.
    """
    p = _payload_from_conf()
    # VP workflow requires both FirstName and LastName on contacts. When
    # the QBO Customer has no person name (company-only record), fall back
    # to DisplayName/Company so the POST passes the workflow rule.
    last_name = (
        p.get('LastName')
        or p.get('FamilyName')
        or p.get('DisplayName')
        or p.get('Company')
        or 'Unknown'
    )
    first_name = (
        p.get('FirstName')
        or p.get('GivenName')
        or p.get('DisplayName')
        or p.get('Company')
        or 'Unknown'
    )
    contact = {
        'ClientID': _firm_id_for_request(),
        'ContactStatus': 'A',
        'FirstName': first_name,
        'MiddleName': p.get('MiddleName') or '',
        'LastName': last_name,
        'Suffix': p.get('Suffix') or '',
        'Title': p.get('Title') or '',
        'Email': p.get('Email') or '',
        'Website': p.get('Website') or '',
        'Phone': p.get('Phone') or '',
        'CellPhone': p.get('Mobile') or '',
        'Fax': p.get('Fax') or '',
        'QBOID': p.get('QBOID') or '',
        'QBOIsMainContact': 'true',
        'FirmAddressDescription': 'Billing',
    }
    # Link to the billing address if VP returned a CLAddress id. The
    # upsert_billing_address task always ran upstream on the success path
    # (it's a required link in the chain) but its result may be empty or
    # the wrong shape; we log a DEBUG trace for diagnosability and treat
    # an absent CLAddress as 'don't add the field' rather than failing.
    try:
        billing = rail.result('upsert_billing_address') or {}
    except Exception:
        logger.debug(
            "Could not read upsert_billing_address result while building "
            "contact body; contact will be created without CLAddress link",
            exc_info=True
        )
        billing = {}
    if isinstance(billing, dict):
        cl_address = (
            billing.get('CLAddressID')
            or billing.get('AddressID')
            or (billing.get('data') or {}).get('CLAddressID')
        )
        if cl_address:
            contact['CLAddress'] = cl_address
    return contact


def build_veaccounting_body():
    """
    POST DVP accounting body. Mirrors `dvp_insert_update_veaccounting`:
    sets the vendor link, 1099 flag, and pay-terms reference for the firm.
    """
    p = _payload_from_conf()
    return {
        'ClientID': _firm_id_for_request(),
        'Vendor': p.get('QBOID') or '',
        'PayTerms': '',
        'Req1099': p.get('Form1099') or 'N',
    }


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Error capture for the create / update leaves. Always RETURNS so the
# router sees the leaf run as SUCCESS and can gather the dict.
# ---------------------------------------------------------------------------
def capture_customer_dag_error(qboid, display_name, fallback_error):
    """Return an error dict when something failed in the leaf; None on clean run."""
    if not fallback_error:
        return None
    return {
        'error': (
            f"QBOID {qboid} ({display_name}) - "
            f"customer leaf failed: {fallback_error}"
        ),
        'QBOID': qboid,
        'DisplayName': display_name,
    }

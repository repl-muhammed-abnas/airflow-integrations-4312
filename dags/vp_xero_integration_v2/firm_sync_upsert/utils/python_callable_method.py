"""
Python callable methods for VP -> Xero Firm Sync Upsert.

Implements all dispatcher and processor callable logic for the 3-DAG pattern:
  - Dispatcher: watermark polling, initial-sync guard, fan-out conf
  - Processor: VP GET + 404 guard, decide_action, Xero body builders,
    map_firm upsert/update, error capture

Porting Workato recipe `014_501_psa_upsert_contact_in_xero` (34 steps).
"""
import logging
import re

import rail

# VantagepointHook.handle_error raises RuntimeError("Failed with Error: <status> - <reason>").
# Anchoring to that prefix avoids false matches on firm IDs or URLs that happen to contain 404.
_VP_404_RE = re.compile(r'Failed with Error:\s+404\b')
from airflow.models import Variable
from vp_xero_integration_v2.common.python_callable_method import (
    build_customer_variable_key,
    collection_single_row,
    collection_upsert,
    collection_update,
    has_sync_errors_method,
    unwrap_vp_response,
    utc_now_iso,
)
from vp_xero_integration_v2.common.tables import (
    MAP_FIRM_TABLE_NAME,
    MAP_FIRM_COLUMNS,
    MAP_FIRM_UNIQUE_COLUMNS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dispatcher callables
# ---------------------------------------------------------------------------

def check_initial_sync_complete_method():
    """Return True when the per-tenant mapping_init Variable is set and truthy.

    Variable key: vp_xero_{customerId}_mapping_init  (e.g. vp_xero_Cust0015_mapping_init)
    - Missing or value 'false'/'0'/'no'/'n' → False  (dispatcher skips to skip_mapping_not_ready)
    - Any truthy value ('true', '1', 'yes', 'y', …)  → True  (proceed with polling flow)

    Ops sets the Variable to 'true' once mapping_sync has completed for a tenant.
    """
    conf = rail.get_current_context()['dag_run'].conf
    customer_id = conf.get('customerId') or ''
    key = build_customer_variable_key(customer_id, 'mapping_init')
    raw = Variable.get(key, default_var=None)
    if raw is None:
        logger.info(
            "Variable '%s' not found — mapping not ready for customer '%s'",
            key, customer_id,
        )
        return False
    result = str(raw).strip().lower() not in ('false', '0', 'no', 'n', '')
    logger.info("Variable '%s' = %r → mapping_ready=%s", key, raw, result)
    return result


def build_firm_poll_filter(**context):
    """Build the VP filterHash query string for the firm poll window.

    Constructs a half-open datetime interval [last_sync_time, current_sync_time)
    on the ModDate field. Called as a callable on VantagepointFirmOperator.filters
    so Jinja never touches it — XCom read happens at execute time.

    No ClientInd=Y filter: Xero syncs BOTH client and vendor firms.
    """
    timestamps = (
        context['ti'].xcom_pull(task_ids='prepare_sync_timestamps')
        or context['ti'].xcom_pull(key='return_value', task_ids='prepare_sync_timestamps')
        or {}
    )
    last_sync = timestamps.get('last_sync_time', '')
    current_sync = timestamps.get('current_sync_time', '')
    return (
        "?filterHash[0][name]=ModDate"
        f"&filterHash[0][value]={last_sync}"
        "&filterHash[0][opp]=%3E%3D"
        "&filterHash[0][seq]=0"
        "&filterHash[0][type]=datetime"
        "&filterHash[1][name]=ModDate"
        f"&filterHash[1][value]={current_sync}"
        "&filterHash[1][opp]=%3C"
        "&filterHash[1][seq]=1"
        "&filterHash[1][type]=datetime"
    )


def extract_firm_list_method():
    """Unwrap the VP firm poll response into a flat list of firm dicts."""
    raw = rail.result('get_recently_changed_firms')
    return unwrap_vp_response(raw)


def check_firms_exist_method():
    """IfOperator test: did the firm poll return at least one firm?"""
    firms = rail.result('extract_firm_list') or []
    return len(firms) > 0


def build_process_firm_conf(item, **_context):
    """Build per-firm DAG conf for TriggerDagRunForEachItemOperator.

    `item` is one firm dict from extract_firm_list. Forwards all fields
    the processor DAG needs plus the connection map from the dispatcher conf.

    NOTE: connections and customerId are injected by the dispatcher's
    create_dag wrapper (V2 architecture); this base function leaves them
    unset so the wrapper can override cleanly.
    """
    dispatcher_conf = rail.get_current_context()['dag_run'].conf or {}
    return {
        'FirmID': item.get('FirmID') or item.get('ClientID'),
        'FirmName': item.get('Name', ''),
        'customerId': dispatcher_conf.get('customerId'),
        'connections': dispatcher_conf.get('connections', {}),
    }


def has_sync_errors():
    """IfOperator test: did any processor child DAG report an error?"""
    return has_sync_errors_method()


# ---------------------------------------------------------------------------
# Processor callables — VP data fetch
# ---------------------------------------------------------------------------

def get_vp_firm_data_method():
    """GET /firm/{FirmID} from VP; return None on 404 (firm deleted in VP).

    Instantiates VantagepointFirmOperator at runtime (not wired as a task)
    so the 404 path can return None without failing the task. Processor
    DAG branches on is_vp_firm_found to route deleted firms into the
    archive-on-delete path.
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf
    firm_id = conf.get('FirmID')
    vp_conn_id = (
        conf.get('connections', {}).get('vantagepoint', 'vantagepoint_default')
    )
    try:
        op = rail.VantagepointFirmOperator(
            task_id='_get_vp_firm_data_inner',
            vp_conn_id=vp_conn_id,
            request_method='GET',
            client_id=str(firm_id),
            pagination=False,
        )
        result = op.execute(context)
        rows = unwrap_vp_response(result)
        return rows[0] if rows else (result if isinstance(result, dict) else None)
    except RuntimeError as exc:
        if _VP_404_RE.search(str(exc)):
            logger.info(
                "VP firm %s not found (404) — treating as deleted", firm_id
            )
            return None
        raise


def is_vp_firm_found_method():
    """IfOperator test: did get_vp_firm_data return a non-None result?"""
    return rail.result('get_vp_firm_data') is not None


# ---------------------------------------------------------------------------
# Processor callables — decision + field derivation
# ---------------------------------------------------------------------------

def _derive_firm_fields(firm, addresses):
    """Derive Xero contact fields from VP firm + address list.

    Returns a dict with all pre-computed body fields so body builders never
    need to re-read XCom or re-derive — one decide_action pass covers all paths.
    """
    client = (firm.get('Client') or '').strip()
    vendor = (firm.get('Vendor') or '').strip()

    # Workato step 12: contact_number = Client || Vendor (first non-empty)
    contact_number = client or vendor or None

    # account_number = SL{Client}[/PL{Vendor}] with empty parts filtered
    parts = []
    if client:
        parts.append(f'SL{client}')
    if vendor:
        parts.append(f'PL{vendor}')
    account_number = '/'.join(parts) or None

    # Workato steps 14-16: TaxRegistrationNumber from the PrimaryInd='Y' address
    tax_number = None
    for addr in (addresses or []):
        if isinstance(addr, dict) and str(addr.get('PrimaryInd', '')).upper() == 'Y':
            tax_number = addr.get('TaxRegistrationNumber') or None
            break

    # STREET address from VP firm flat PrimaryAddress* fields
    street_addr = {
        'AddressType': 'STREET',
        'AddressLine1': firm.get('PrimaryAddress1') or '',
        'AddressLine2': firm.get('PrimaryAddress2') or '',
        'City': firm.get('PrimaryCity') or '',
        'Region': firm.get('PrimaryState') or '',
        'PostalCode': firm.get('PrimaryZip') or '',
        'Country': firm.get('PrimaryCountry') or '',
    }

    # POBOX: PaymentAddress preferred; fall back to BillingAddress
    payment_has_data = bool(
        firm.get('PaymentAddress1') or firm.get('PaymentCity')
    )
    if payment_has_data:
        pobox_addr = {
            'AddressType': 'POBOX',
            'AddressLine1': firm.get('PaymentAddress1') or '',
            'AddressLine2': firm.get('PaymentAddress2') or '',
            'City': firm.get('PaymentCity') or '',
            'Region': firm.get('PaymentState') or '',
            'PostalCode': firm.get('PaymentZip') or '',
            'Country': firm.get('PaymentCountry') or '',
        }
    else:
        pobox_addr = {
            'AddressType': 'POBOX',
            'AddressLine1': firm.get('BillingAddress1') or '',
            'AddressLine2': firm.get('BillingAddress2') or '',
            'City': firm.get('BillingCity') or '',
            'Region': firm.get('BillingState') or '',
            'PostalCode': firm.get('BillingZip') or '',
            'Country': firm.get('BillingCountry') or '',
        }

    return {
        'Name': firm.get('Name'),
        'EmailAddress': firm.get('PrimaryEmail') or None,
        'phone_number': firm.get('PrimaryPhone') or None,
        'ContactNumber': contact_number,
        'AccountNumber': account_number,
        'TaxNumber': tax_number,
        'street_addr': street_addr,
        'pobox_addr': pobox_addr,
        'vp_status': firm.get('Status'),
        'vp_client': client,
        'vp_vendor': vendor,
    }


def decide_action_method():
    """Compute action + all Xero body fields; return as a dict.

    Reads get_vp_firm_data and get_vp_addresses XCom, looks up map_firm,
    and returns a rich dict containing:
      - 'action': 'create' | 'update' | 'skip_not_ready' | 'skip_archived'
      - all pre-computed Xero body fields (used by body builder callables)
      - 'ContactID': present for update/skip_archived paths
      - 'map_row': the existing map_firm row dict (or None)

    Steps 2–23 of the Workato recipe condensed to one Python pass.
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf
    firm_id = str(conf.get('FirmID') or '')

    firm = rail.result('get_vp_firm_data') or {}

    # Workato step 9: ReadyForApproval gate
    # VP returns "Y"/"N"; guard also accepts "true"/"1"/"yes" for resilience.
    ready = firm.get('ReadyForApproval')
    if str(ready).lower() not in ('y', 'true', '1', 'yes'):
        logger.info("Firm %s: ReadyForApproval=%s — skip_not_ready", firm_id, ready)
        return {'action': 'skip_not_ready', 'FirmID': firm_id}

    raw_addresses = rail.result('get_vp_addresses')
    addresses = unwrap_vp_response(raw_addresses)

    fields = _derive_firm_fields(firm, addresses)
    fields['FirmID'] = firm_id

    # Workato step 11: look up map_firm row by FirmID
    query = (
        f"SELECT {', '.join(MAP_FIRM_COLUMNS)} FROM {MAP_FIRM_TABLE_NAME} "
        f"WHERE FirmID = ? LIMIT 1"
    )
    map_row = collection_single_row(
        query, [firm_id],
        context=context,
        read_task_id='_decide_lookup_map_firm',
    )

    # Workato step 17: no map row OR ContactID blank → CREATE
    if not map_row or not (map_row.get('ContactID') or '').strip():
        logger.info("Firm %s: no map_firm row — action=create", firm_id)
        return {**fields, 'action': 'create', 'map_row': map_row}

    # Map row exists; check if contact is already archived (skip — Xero
    # does not allow editing archived contacts to reactivate them)
    map_status = (map_row.get('Status') or '').upper()
    if map_status == 'ARCHIVED':
        logger.info(
            "Firm %s: map_firm Status=ARCHIVED — skip_archived (Xero "
            "does not allow reactivating archived contacts)",
            firm_id,
        )
        return {
            **fields, 'action': 'skip_archived',
            'ContactID': map_row['ContactID'],
            'ContactStatus': 'ARCHIVED',
            'map_row': map_row,
        }

    # UPDATE path: VP Status A → ACTIVE, anything else → ARCHIVED
    xero_status = 'ACTIVE' if firm.get('Status') == 'A' else 'ARCHIVED'
    logger.info(
        "Firm %s: map_firm found (ContactID=%s) — action=update ContactStatus=%s",
        firm_id, map_row['ContactID'], xero_status,
    )
    return {
        **fields,
        'action': 'update',
        'ContactID': map_row['ContactID'],
        'ContactStatus': xero_status,
        'map_row': map_row,
    }


def is_create_action_method():
    """IfOperator test: did decide_action return action='create'?"""
    decision = rail.result('decide_action') or {}
    return decision.get('action') == 'create'


def is_update_action_method():
    """IfOperator test: did decide_action return action='update'?"""
    decision = rail.result('decide_action') or {}
    return decision.get('action') == 'update'


# ---------------------------------------------------------------------------
# Xero body builder callables (called with **context by XeroContactOperator)
# ---------------------------------------------------------------------------

def _build_addresses(d):
    """Return a 1-or-2-element Addresses list, omitting blank entries."""
    addresses = []
    for addr in (d.get('street_addr'), d.get('pobox_addr')):
        if addr and isinstance(addr, dict):
            clean = {k: v for k, v in addr.items() if v}
            if len(clean) > 1:  # more than just AddressType
                addresses.append(clean)
    return addresses


def _build_phones(d):
    """Return Phones list when a phone number is present."""
    phone = d.get('phone_number')
    if not phone:
        return []
    return [{'PhoneType': 'DEFAULT', 'PhoneNumber': str(phone)}]


def build_create_contact_body(**_context):
    """Xero POST /Contacts body for the create path.

    Called as XeroContactOperator.request_body callable. Reads decide_action
    XCom to avoid re-deriving all fields.
    """
    d = rail.result('decide_action') or {}
    contact = {
        'Name': d.get('Name'),
        'ContactStatus': 'ACTIVE',
        'EmailAddress': d.get('EmailAddress'),
        'ContactNumber': d.get('ContactNumber'),
        'AccountNumber': d.get('AccountNumber'),
        'TaxNumber': d.get('TaxNumber'),
        'Addresses': _build_addresses(d),
        'Phones': _build_phones(d),
    }
    contact = {k: v for k, v in contact.items() if v is not None and v != [] and v != ''}
    return {'Contacts': [contact]}


def build_update_contact_body(**_context):
    """Xero POST /Contacts body for the update path.

    Same as create but includes ContactID (required for update) and
    ContactStatus derived from VP Status (A → ACTIVE, else → ARCHIVED).
    """
    d = rail.result('decide_action') or {}
    contact = {
        'ContactID': d.get('ContactID'),
        'Name': d.get('Name'),
        'ContactStatus': d.get('ContactStatus', 'ACTIVE'),
        'EmailAddress': d.get('EmailAddress'),
        'ContactNumber': d.get('ContactNumber'),
        'AccountNumber': d.get('AccountNumber'),
        'TaxNumber': d.get('TaxNumber'),
        'Addresses': _build_addresses(d),
        'Phones': _build_phones(d),
    }
    contact = {k: v for k, v in contact.items() if v is not None and v != [] and v != ''}
    return {'Contacts': [contact]}


def build_archive_from_delete_body(**_context):
    """Xero POST /Contacts body to archive a contact when its VP firm was deleted.

    Reads lookup_map_firm_for_delete XCom for the ContactID.
    """
    map_row = rail.result('lookup_map_firm_for_delete') or {}
    contact_id = map_row.get('ContactID')
    return {'Contacts': [{'ContactID': contact_id, 'ContactStatus': 'ARCHIVED'}]}


# ---------------------------------------------------------------------------
# Processor callables — map_firm writes
# ---------------------------------------------------------------------------

def _contact_id_from_xero_response(task_id):
    """Extract ContactID from a XeroContactOperator create/update response.

    Response shape: {'success': True, 'data': [{'ContactID': '...', ...}], ...}
    """
    response = rail.result(task_id) or {}
    if not isinstance(response, dict):
        return None
    data = response.get('data')
    if isinstance(data, list) and data:
        return (data[0] or {}).get('ContactID')
    if isinstance(data, dict):
        return data.get('ContactID')
    return None


def upsert_map_firm_after_create_method():
    """Write map_firm row after a successful Xero contact create.

    Reads ContactID from create_xero_contact response, all other fields
    from decide_action. Keyed by ContactID (UNIQUE index on map_firm).
    """
    context = rail.get_current_context()
    contact_id = _contact_id_from_xero_response('create_xero_contact')
    if not contact_id:
        logger.warning(
            "upsert_map_firm_after_create: no ContactID in create_xero_contact "
            "response — skipping map_firm write"
        )
        return None

    d = rail.result('decide_action') or {}
    firm_id = d.get('FirmID') or context['dag_run'].conf.get('FirmID')
    now = utc_now_iso()

    collection_upsert(
        MAP_FIRM_TABLE_NAME,
        key_columns=MAP_FIRM_UNIQUE_COLUMNS,
        data_columns={
            'FirmID': str(firm_id),
            'ContactID': contact_id,
            'Status': 'ACTIVE',
            'Vendor': d.get('vp_vendor', ''),
            'Client': d.get('vp_client', ''),
            'XeroName': d.get('Name', ''),
            'VantagepointName': d.get('Name', ''),
            'ModDate': now,
        },
        context=context,
    )
    logger.info(
        "map_firm upserted after create: FirmID=%s ContactID=%s",
        firm_id, contact_id,
    )
    return contact_id


def upsert_map_firm_after_update_method():
    """Refresh map_firm row after a successful Xero contact update.

    Updates Status, Vendor, Client, XeroName, VantagepointName, ModDate
    keyed by ContactID. Mirrors Workato step 26.
    """
    context = rail.get_current_context()
    d = rail.result('decide_action') or {}
    contact_id = d.get('ContactID')
    firm_id = d.get('FirmID') or context['dag_run'].conf.get('FirmID')

    if not contact_id:
        logger.warning(
            "upsert_map_firm_after_update: no ContactID in decide_action — "
            "skipping map_firm write"
        )
        return None

    now = utc_now_iso()
    new_status = d.get('ContactStatus', 'ACTIVE')

    collection_upsert(
        MAP_FIRM_TABLE_NAME,
        key_columns=MAP_FIRM_UNIQUE_COLUMNS,
        data_columns={
            'FirmID': str(firm_id),
            'ContactID': contact_id,
            'Status': new_status,
            'Vendor': d.get('vp_vendor', ''),
            'Client': d.get('vp_client', ''),
            'XeroName': d.get('Name', ''),
            'VantagepointName': d.get('Name', ''),
            'ModDate': now,
        },
        context=context,
    )
    logger.info(
        "map_firm upserted after update: FirmID=%s ContactID=%s Status=%s",
        firm_id, contact_id, new_status,
    )
    return contact_id


# ---------------------------------------------------------------------------
# Processor callables — VP not-found / delete path
# ---------------------------------------------------------------------------

def lookup_map_firm_for_delete_method():
    """Look up map_firm row by FirmID when VP returned 404 (firm deleted).

    Returns the map_firm row dict or None.
    """
    context = rail.get_current_context()
    conf = context['dag_run'].conf
    firm_id = str(conf.get('FirmID') or '')
    query = (
        f"SELECT {', '.join(MAP_FIRM_COLUMNS)} FROM {MAP_FIRM_TABLE_NAME} "
        f"WHERE FirmID = ? LIMIT 1"
    )
    return collection_single_row(
        query, [firm_id],
        context=context,
        read_task_id='_lookup_map_firm_for_delete',
    )


def is_in_map_for_delete_method():
    """IfOperator test: did lookup_map_firm_for_delete find a row with a ContactID?"""
    row = rail.result('lookup_map_firm_for_delete') or {}
    return bool(row.get('ContactID'))


def update_map_archived_method():
    """Mark map_firm row Status='ARCHIVED' after archiving the Xero contact.

    Mirrors Workato step 31: update col3=ARCHIVED, col8=now.
    """
    context = rail.get_current_context()
    map_row = rail.result('lookup_map_firm_for_delete') or {}
    contact_id = map_row.get('ContactID')
    firm_id = map_row.get('FirmID') or context['dag_run'].conf.get('FirmID')

    if not contact_id:
        logger.warning(
            "update_map_archived: no ContactID in lookup_map_firm_for_delete "
            "— skipping map_firm update"
        )
        return None

    now = utc_now_iso()
    collection_update(
        MAP_FIRM_TABLE_NAME,
        query=(
            f"UPDATE {MAP_FIRM_TABLE_NAME} "
            f"SET Status = 'ARCHIVED', ModDate = ? "
            f"WHERE ContactID = ?"
        ),
        params=[now, contact_id],
        context=context,
    )
    logger.info(
        "map_firm marked ARCHIVED: FirmID=%s ContactID=%s", firm_id, contact_id
    )
    return contact_id


# ---------------------------------------------------------------------------
# Processor callables — skip / no-op paths
# ---------------------------------------------------------------------------

def log_skip_method():
    """Log the skip reason from decide_action (skip_not_ready or skip_archived)."""
    decision = rail.result('decide_action') or {}
    action = decision.get('action', 'unknown')
    firm_id = decision.get('FirmID') or rail.get_current_context()['dag_run'].conf.get('FirmID')
    logger.info(
        "Firm %s: skipping — action=%s", firm_id, action
    )


def log_skip_not_in_map_method():
    """Log that the deleted VP firm has no map_firm entry (nothing to archive)."""
    conf = rail.get_current_context()['dag_run'].conf
    firm_id = conf.get('FirmID')
    logger.info(
        "Firm %s: VP 404 but no map_firm entry — no Xero contact to archive",
        firm_id,
    )


# ---------------------------------------------------------------------------
# Processor callables — error capture
# ---------------------------------------------------------------------------

def capture_processor_error(firm_id, firm_name, error_message):
    """Return error dict; NEVER raises — keeps processor DAG SUCCESS.

    Wired on catch_processor_dag_error (trigger_rule='one_failed'). On the
    happy path this task is SKIPPED, so GatherResultsFromDagRunsOperator
    only aggregates real failures.
    """
    label = (
        f"Firm {firm_id} ({str(firm_name).strip()})"
        if firm_name and str(firm_name).strip()
        else f"Firm {firm_id}"
    )
    return {'error': f"{label} - sync failed: {error_message}"}

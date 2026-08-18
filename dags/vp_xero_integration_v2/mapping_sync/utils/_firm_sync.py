"""
Firm mapping sync (Xero Contacts → VP Firms).

Direction: Xero → Vantagepoint (Xero is master). Reproduces the Workato
`014_501_psa_synch_firms` recipe with the `Map Firms` seeder folded in
(Option A — see reverse-engineering docs 01-synch-firms.md + 06-lookup-table-seeding.md).

Per Xero contact not already mapped (anti-join on ContactID):
  - Match by Name to an existing VP firm. If matched, reuse MIN(ClientID) — no
    create (Q4 = A). If no match, create the VP firm and its addresses.
  - Derive Vendor/Client codes from the Xero AccountNumber (`SL<client>/PL<vendor>`)
    and set ClientInd/VendorInd from IsCustomer/IsSupplier — fixes the Workato
    seeder gap that left Vendor/Client blank (Q9).
  - Upsert the map_firm row keyed on ContactID (INSERT OR REPLACE parity).

Fixes vs Workato (Q9 / doc 06):
  - Vendor/Client derived during the sync (Workato seeder left them blank).
  - All Xero list reads paginate (XeroContactOperator paginate=True) — Workato
    did not page /Contacts.
  - Address creation is deduped by (ClientID, AddressType, Address1) so a
    re-created firm does not accumulate duplicate addresses (Workato used a
    fresh CLAddressID uuid each run with no dedup).

Public surface (re-exported via `python_callable_method.py`):
    sync_xero_firms_to_vp
"""
import logging
import uuid

import rail

from vp_xero_integration_v2.mapping_sync.utils._shared import (
    _filter_none,
    _extract_xero_records,
    _extract_vp_client_id,
)
from vp_xero_integration_v2.common.python_callable_method import unwrap_vp_response
from vp_xero_integration_v2.common.tables import (
    MAP_FIRM_TABLE_NAME,
    MAP_FIRM_UNIQUE_COLUMNS,
)
from vp_xero_integration_v2.mapping_sync.config import IntegrationConfig

_log = logging.getLogger(__name__)


# ===========================================================================
# FIRM MAPPING — field helpers / body builders (Xero Contact → VP Firm)
# ===========================================================================

def _xero_status_to_vp_status(contact_status):
    """Xero ContactStatus → VP firm Status ('A'/'I')."""
    if not contact_status:
        return 'A'
    return 'I' if str(contact_status).strip().upper() == 'ARCHIVED' else 'A'


def _parse_account_number(account_number):
    """Parse a Xero Contact AccountNumber into (client_code, vendor_code).

    Convention (Workato synch_firms step 16/21): `SL<client>/PL<vendor>`.
      - Client = segment before '/', minus a leading 'SL'.
      - Vendor = segment after '/', minus a leading 'PL'.
    Returns ('', '') when the AccountNumber is blank or a segment is absent.
    """
    if not account_number:
        return '', ''
    parts = str(account_number).split('/')
    client = parts[0].strip() if parts and parts[0] else ''
    vendor = parts[1].strip() if len(parts) > 1 and parts[1] else ''
    if client[:2].upper() == 'SL':
        client = client[2:]
    if vendor[:2].upper() == 'PL':
        vendor = vendor[2:]
    return client, vendor


def _xero_contact_phone(contact):
    """Best phone number for a Xero contact: PhoneType DDI, else DEFAULT
    (Workato synch_firms step 4)."""
    phones = contact.get('Phones') or []
    by_type = {}
    for phone in phones:
        if not isinstance(phone, dict):
            continue
        number = (phone.get('PhoneNumber') or '').strip()
        if number:
            by_type[(phone.get('PhoneType') or '').upper()] = number
    return by_type.get('DDI') or by_type.get('DEFAULT')


def build_vp_firm_create_body(contact, default_org, client_code, vendor_code):
    """POST /firm body for a net-new VP firm created from a Xero contact.

    Field shape per Workato synch_firms step 21: Name/SortName from the Xero
    contact Name; ClientInd/VendorInd from IsCustomer/IsSupplier; Client/Vendor
    from the parsed AccountNumber; Status from ContactStatus; default Org; the
    standard new-firm flags. None-valued keys are dropped (VP rejects some empty
    strings — e.g. an empty Org/Relationship).
    """
    name = contact.get('Name')
    body = {
        'Name': name,
        'SortName': name,
        'Status': _xero_status_to_vp_status(contact.get('ContactStatus')),
        'ClientInd': 'Y' if contact.get('IsCustomer') else 'N',
        'VendorInd': 'Y' if contact.get('IsSupplier') else 'N',
        'Client': client_code or None,
        'Vendor': vendor_code or None,
        'Org': default_org or None,
        'AvailableForCRM': 'N',
        'ReadyForApproval': True,
        'ReadyForProcessing': 'N',
    }
    return _filter_none(body)


def build_vp_firm_address_bodies(contact, country_index, state_index):
    """Build the VP firm-address bodies for a Xero contact's STREET ∪ POBOX
    addresses (Workato synch_firms step 32/34).

    Returns a list of address-body dicts (no client_id — the caller posts each
    to /firm/{ClientID}/address). Country/State are resolved from VP codetable
    indexes (Description → Code); unresolved values pass through unchanged.
    A fresh CLAddressID uuid is assigned per address.
    """
    addresses = contact.get('Addresses') or []
    phone = _xero_contact_phone(contact)
    email = contact.get('EmailAddress')
    tax_number = contact.get('TaxNumber')
    bodies = []
    for address in addresses:
        if not isinstance(address, dict):
            continue
        address_type = (address.get('AddressType') or '').strip().upper()
        if address_type not in ('STREET', 'POBOX'):
            continue
        country_name = address.get('Country')
        region_name = address.get('Region')
        body = {
            'CLAddressID': str(uuid.uuid4()),
            'AddressType': address_type,
            # STREET is the primary address; POBOX is the billing/accounting one.
            'PrimaryInd': 'true' if address_type == 'STREET' else 'false',
            'Billing': 'true' if address_type == 'POBOX' else 'false',
            'Payment': 'true' if address_type == 'POBOX' else 'false',
            'Address1': address.get('AddressLine1'),
            'Address2': address.get('AddressLine2'),
            'Address3': address.get('AddressLine3'),
            'Address4': address.get('AddressLine4'),
            'City': address.get('City'),
            'Zip': address.get('PostalCode'),
            'State': _resolve_code(state_index, region_name),
            'Country': _resolve_code(country_index, country_name),
            'Email': email,
            'TaxRegistrationNumber': tax_number,
            'Phone': phone,
        }
        bodies.append(_filter_none(body))
    return bodies


def _resolve_code(index, description):
    """Resolve a VP codetable Code from its Description via `index`
    (Description.lower() → Code). Unknown / blank descriptions pass through
    unchanged so VP surfaces the validation error rather than us silently
    dropping the value."""
    if not description:
        return None
    return index.get(str(description).strip().lower(), description)


# ===========================================================================
# Bulk loaders (one VP round-trip each; indexed in memory)
# ===========================================================================

def _load_vp_firms_by_name(vp_conn_id, context):
    """Bulk-load every VP firm and index by Name → the MIN(ClientID) firm.

    Mirrors Workato synch_firms step 13's `MIN(vf.ClientID)` collapse: when
    several VP firms share a Name, the lexicographically smallest ClientID wins
    so a Xero contact maps deterministically to one firm. Returns
    `dict[name -> {'ClientID': <min>, 'Name': <name>}]`.
    """
    from rail import VantagepointFirmOperator  # pylint: disable=import-outside-toplevel

    result = VantagepointFirmOperator(
        task_id='_bulk_get_vp_firms_for_name_index',
        vp_conn_id=vp_conn_id,
        request_method='GET',
        pagination=True,
    ).execute(context)

    records = unwrap_vp_response(result)
    index = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        name = record.get('Name')
        client_id = record.get('ClientID')
        if not name or client_id in (None, ''):
            continue
        key = name.strip().lower()
        existing = index.get(key)
        if existing is None or str(client_id) < str(existing['ClientID']):
            index[key] = {'ClientID': str(client_id), 'Name': name}
    return index


def _load_default_org(vp_conn_id, context):
    """Return the default VP Org code (the first organization) for new firms
    (Workato synch_firms step 12)."""
    from rail import VantagepointOrganizationOperator  # pylint: disable=import-outside-toplevel

    result = VantagepointOrganizationOperator(
        task_id='_get_default_org',
        vp_conn_id=vp_conn_id,
        request_method='GET',
        pagination=False,
    ).execute(context)
    records = unwrap_vp_response(result)
    for record in records:
        if isinstance(record, dict):
            org = record.get('Org') or record.get('Organization')
            if org:
                return org
    return None


def _load_codetable_index(vp_conn_id, codetable_object, context):
    """Load a VP codetable and index Description.lower() → Code.

    Used for FW_CFGCountry and CFGStates so Xero Country/Region names resolve to
    VP codes during address creation (Workato synch_firms steps 28-31)."""
    from rail import VantagepointCodetableRecordsOperator  # pylint: disable=import-outside-toplevel

    result = VantagepointCodetableRecordsOperator(
        task_id=f'_get_codetable_{codetable_object.lower()}',
        vp_conn_id=vp_conn_id,
        request_method='GET',
        codetable_object=codetable_object,
        pagination=True,
    ).execute(context)
    records = unwrap_vp_response(result)
    index = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        description = record.get('Description')
        code = record.get('Code')
        if description and code not in (None, ''):
            index[str(description).strip().lower()] = code
    return index


def _load_existing_map_firm_index(cur):
    """Read map_firm rows from the open sqlite cursor, indexed by ContactID.

    Used for the anti-join — a contact whose ContactID already has a populated
    FirmID is skipped (already mapped). Mirrors Workato's "process only
    blank-FirmID rows" eligibility filter, generalised to "skip already-mapped".
    """
    cur.execute(
        f'SELECT FirmID, ContactID, Status, Vendor, Client, XeroName, '
        f'VantagepointName, ModDate FROM {MAP_FIRM_TABLE_NAME}'
    )
    index = {}
    for row in cur.fetchall():
        contact_id = row[1]
        if contact_id:
            index[str(contact_id)] = {
                'FirmID': row[0],
                'ContactID': contact_id,
                'Status': row[2],
            }
    return index


def _build_map_firm_row(*, firm_id, contact_id, status, vendor, client,
                        xero_name, vantagepoint_name, mod_date):
    """Assemble one map_firm row dict for the batched upsert. Keys cover every
    MAP_FIRM_COLUMNS entry (the upsert builds its ON CONFLICT statement from the
    first row's keys, so all rows share this exact column set). Natural key is
    ContactID (MAP_FIRM_UNIQUE_COLUMNS)."""
    return {
        'FirmID': firm_id,
        'ContactID': contact_id,
        'Status': status,
        'Vendor': vendor,
        'Client': client,
        'XeroName': xero_name,
        'VantagepointName': vantagepoint_name,
        'ModDate': mod_date,
    }


def sync_xero_firms_to_vp(instance):  # pylint: disable=too-many-locals,too-many-statements
    """For each fetched Xero contact not already mapped, match it to a VP firm
    by Name (reusing MIN(ClientID)) or create the VP firm + addresses, then
    upsert the map_firm cross-reference row keyed on ContactID.

    Called by map_firm_dag's `process_xero_firms` PythonOperator.

    Reads:
      - rail.result('fetch_xero_contacts') — list of Xero Contact records
        (paginated, active + archived).

    Returns a summary dict {matched, created, skipped_existing,
    addresses_created, errors}. Raises RuntimeError at the end if any per-record
    failure occurred (so the DAG's catch task fires). Successful rows are
    committed to S3 before the raise.
    """
    import sqlite3  # pylint: disable=import-outside-toplevel
    import rail.lib.s3_collection  # pylint: disable=import-outside-toplevel
    from rail import (  # pylint: disable=import-outside-toplevel
        S3UpsertCollectionOperator,
        VantagepointFirmOperator,
        VantagepointFirmAddressOperator,
    )

    context = rail.get_current_context()
    log = context['task_instance'].log

    contacts = _extract_xero_records(rail.result('fetch_xero_contacts'))
    log.info("Processing %d Xero contacts → VP firms", len(contacts))

    if not contacts:
        log.warning(
            "Xero returned 0 contacts — skipping map_firm sync. "
            "Verify the Xero connection points to the correct tenant."
        )
        return {'matched': 0, 'created': 0, 'skipped_existing': 0,
                'addresses_created': 0, 'errors': []}

    conn_ids = IntegrationConfig.get_conn_ids(context)
    vp_conn_id = conn_ids['vp_conn_id']

    s3_integration = IntegrationConfig.S3_INTEGRATION_NAME
    s3_customer = IntegrationConfig.get_s3_customer(context)
    s3_integration_type = IntegrationConfig.get_s3_integration_type(context)
    s3_artifact_name = rail.lib.s3_collection.get_s3_collection_artifact_name(
        context, s3_integration, s3_customer, s3_integration_type
    )

    summary = {'matched': 0, 'created': 0, 'skipped_existing': 0,
               'addresses_created': 0, 'errors': []}

    # ---- Phase 0: load existing map_firm index (lock-free read) ----
    existing_map = {}
    with rail.lib.s3_collection.get_or_create_s3_collection_artifact(
        s3_artifact_name, s3_integration, s3_customer, context,
        integration_type=s3_integration_type, use_lock=False,
    ) as artifact:
        with sqlite3.connect(artifact.local_filename) as conn:
            existing_map = _load_existing_map_firm_index(conn.cursor())

    # ---- Phase 1: VP API work, accumulate map rows in memory (no S3 lock) ----
    vp_firms_by_name = _load_vp_firms_by_name(vp_conn_id, context)
    log.info("Loaded %d VP firm name(s) for matching", len(vp_firms_by_name))

    # Country/state codetables + default org are only needed when we create a
    # firm; load lazily on first net-new contact to avoid the round-trips on
    # already-fully-mapped re-runs.
    lazy = {'default_org': None, 'country_index': None,
            'state_index': None, 'loaded': False}

    def _ensure_create_refs():
        if not lazy['loaded']:
            lazy['default_org'] = _load_default_org(vp_conn_id, context)
            lazy['country_index'] = _load_codetable_index(
                vp_conn_id, 'FW_CFGCountry', context)
            lazy['state_index'] = _load_codetable_index(
                vp_conn_id, 'CFGStates', context)
            lazy['loaded'] = True

    from datetime import datetime, timezone  # pylint: disable=import-outside-toplevel
    mod_date = (
        datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    )

    map_rows = []
    seen_addresses = set()

    def _create_firm_addresses(client_id, contact):
        _ensure_create_refs()
        bodies = build_vp_firm_address_bodies(
            contact, lazy['country_index'], lazy['state_index'],
        )
        for body in bodies:
            dedup_key = (client_id, body.get('AddressType'), body.get('Address1'))
            if dedup_key in seen_addresses:
                continue
            seen_addresses.add(dedup_key)
            VantagepointFirmAddressOperator(
                task_id=f'_post_firm_address_{client_id}_{body.get("AddressType")}',
                vp_conn_id=vp_conn_id,
                request_method='POST',
                client_id=client_id,
                request_body=body,
                pagination=False,
            ).execute(context)
            summary['addresses_created'] += 1

    def _process_one(contact):
        contact_id = contact.get('ContactID')
        name = contact.get('Name') or ''

        if not contact_id:
            summary['errors'].append({
                'contact_id': None, 'name': name,
                'error': 'Xero contact has no ContactID field',
            })
            return

        # Anti-join: skip contacts already mapped to a VP firm.
        existing = existing_map.get(str(contact_id))
        if existing and existing.get('FirmID'):
            summary['skipped_existing'] += 1
            return

        try:
            client_code, vendor_code = _parse_account_number(
                contact.get('AccountNumber'))
            status = contact.get('ContactStatus')

            vp_firm = vp_firms_by_name.get(name.strip().lower())
            if vp_firm and vp_firm.get('ClientID'):
                # Existing-by-name: reuse MIN(ClientID), no create.
                client_id = vp_firm['ClientID']
                vantagepoint_name = vp_firm.get('Name') or name
                summary['matched'] += 1
            else:
                # Net-new: create the VP firm, then its addresses.
                _ensure_create_refs()
                create_body = build_vp_firm_create_body(
                    contact, lazy['default_org'], client_code, vendor_code)
                create_result = VantagepointFirmOperator(
                    task_id=f'_post_firm_{contact_id}',
                    vp_conn_id=vp_conn_id,
                    request_method='POST',
                    request_body=create_body,
                    pagination=False,
                ).execute(context)
                client_id = _extract_vp_client_id(create_result)
                if not client_id:
                    raise RuntimeError(
                        f"VP firm create for contact {contact_id} ({name}) "
                        f"returned no ClientID")
                vantagepoint_name = name
                summary['created'] += 1
                _create_firm_addresses(client_id, contact)

            map_rows.append(_build_map_firm_row(
                firm_id=client_id,
                contact_id=str(contact_id),
                status=status,
                vendor=vendor_code,
                client=client_code,
                xero_name=name,
                vantagepoint_name=vantagepoint_name,
                mod_date=mod_date,
            ))

        except Exception as exc:  # pylint: disable=broad-exception-caught
            log.error("Failed to sync Xero contact %s (%s): %s",
                      contact_id, name, exc)
            summary['errors'].append({
                'contact_id': contact_id, 'name': name, 'error': str(exc),
            })

    for contact in contacts:
        _process_one(contact)

    # ---- Phase 2: single batched upsert (one S3 lock cycle), keyed ContactID
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

    log.info("map_firm sync summary: %s", summary)
    if summary['errors']:
        raise RuntimeError(
            f"map_firm sync had {len(summary['errors'])} failure(s); "
            f"first: {summary['errors'][0]}"
        )
    return summary

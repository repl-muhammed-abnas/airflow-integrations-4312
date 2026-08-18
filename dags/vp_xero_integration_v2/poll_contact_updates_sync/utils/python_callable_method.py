"""
Utility callables for Xero -> VP Poll Contact Updates Sync.

Migrates Workato `014-501 PSA Poll Xero Contact updates Vantagepoint`:
  - Step 1: Lookup map_employee by ContactID; skip if found (employee filter).
  - Step 3: Lookup map_firm by ContactID; get stored ModDate.
  - Step 4: Sync only if no firm map row OR Xero UpdatedDateUTC > stored ModDate.
  - Step 5: Invoke firm sync (create/update VP firm + upsert map_firm row).

The firm sync reuses building blocks from mapping_sync._firm_sync but skips the
anti-join guard — poll_contact_updates must refresh existing firms when their
Xero timestamp is newer (unlike the initial seeder, which is create-only).

Source recipe:
  `014_501_psa_poll_xero_contact_updates_vantagepoint.recipe.json`
  (Vantagepoint-Quickbooks-Migration repo, integration_vantagepoint_xero/
  code/014-501 PSA/Triggers - Polling/)
"""
# pylint: disable=invalid-name,too-many-locals
import logging
from datetime import datetime, timezone

import rail

from vp_xero_integration_v2.common.python_callable_method import (
    watermark_key_template,
    prepare_sync_timestamps,
    update_last_sync_time,
    collection_single_row,
    MAPPING_COLLECTION_INTEGRATION_TYPE,
)
from vp_xero_integration_v2.common.tables import (
    MAP_EMPLOYEE_TABLE_NAME,
    MAP_FIRM_TABLE_NAME,
    MAP_FIRM_UNIQUE_COLUMNS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Watermark helpers
# ---------------------------------------------------------------------------
WATERMARK_VARIABLE_KEY_TEMPLATE = watermark_key_template('poll_contact_updates')


def prepare_sync_timestamps_method(instance, fallback_initial_sync_time):
    """Bind the integration-specific watermark template for prepare_sync_timestamps."""
    return prepare_sync_timestamps(
        instance, WATERMARK_VARIABLE_KEY_TEMPLATE, fallback_initial_sync_time
    )


def update_last_sync_time_method(instance):
    return update_last_sync_time(instance, WATERMARK_VARIABLE_KEY_TEMPLATE)


# ---------------------------------------------------------------------------
# Employee map lookup (Step 1 / Step 2 in Workato recipe)
# ---------------------------------------------------------------------------
# NOTE: map_employee is seeded by an out-of-scope employee-sync integration and
# is typically empty. collection_single_row returns None for a missing/empty table,
# so this guard is a no-op until employee sync is wired up. The check is retained
# to faithfully mirror the Workato recipe and to activate automatically once seeding
# is in place.
def check_employee_map_method():
    """Return the map_employee row for this contact, or None.

    Called by `check_employee_map` PythonOperator. Returns a non-None dict
    if the ContactID is a known Xero employee — the processor skips sync.
    Treats a missing collection or table as 'not an employee'.
    """
    contact_id = rail.get_current_context()['dag_run'].conf.get('ContactID')
    if not contact_id:
        logger.warning("No ContactID in dag_run.conf; skipping employee check")
        return None
    return collection_single_row(
        f'SELECT ContactID, Employee FROM {MAP_EMPLOYEE_TABLE_NAME} '
        'WHERE ContactID = ?',
        [str(contact_id)],
        read_task_id='check_employee_map',
    )


def is_employee_contact_method():
    """IfOperator test: True when the contact is a mapped Xero employee."""
    result = rail.result('check_employee_map')
    is_employee = bool(result)
    if is_employee:
        logger.info(
            "Contact %s is a mapped employee — skipping firm sync",
            (result or {}).get('ContactID'),
        )
    return is_employee


# ---------------------------------------------------------------------------
# Firm map lookup + change-detection (Steps 3-4 in Workato recipe)
# ---------------------------------------------------------------------------
def check_firm_map_method():
    """Return the map_firm row for this contact, or None.

    Called by `check_firm_map` PythonOperator. Returns a dict with at least
    `ModDate` if the contact has been previously synced as a VP firm, else None.
    """
    contact_id = rail.get_current_context()['dag_run'].conf.get('ContactID')
    if not contact_id:
        return None
    return collection_single_row(
        f'SELECT ContactID, FirmID, ModDate FROM {MAP_FIRM_TABLE_NAME} '
        'WHERE ContactID = ?',
        [str(contact_id)],
        read_task_id='check_firm_map',
    )


def firm_needs_sync_method():
    """IfOperator test: True when the firm should be synced.

    Mirrors Workato Step 4 condition:
      firm NOT in map_firm  →  sync (new firm)
      UpdatedDateUTC > map_firm.ModDate  →  sync (Xero is newer)
      else  →  skip (already up to date)
    """
    firm_row = rail.result('check_firm_map')
    if not firm_row:
        logger.info("No existing firm map row — syncing new firm")
        return True

    updated_utc_str = (
        rail.get_current_context()['dag_run'].conf.get('UpdatedDateUTC', '')
    )
    mod_date_str = firm_row.get('ModDate', '')
    if not updated_utc_str or not mod_date_str:
        logger.info(
            "Missing timestamp(s) — UpdatedDateUTC=%r ModDate=%r; defaulting to sync",
            updated_utc_str, mod_date_str,
        )
        return True

    try:
        updated_dt = _parse_iso(updated_utc_str)
        mod_dt = _parse_iso(mod_date_str)
        needs_sync = updated_dt > mod_dt
        logger.info(
            "UpdatedDateUTC=%s  ModDate=%s  needs_sync=%s",
            updated_utc_str, mod_date_str, needs_sync,
        )
        return needs_sync
    except (ValueError, TypeError) as exc:
        logger.warning("Timestamp comparison failed (%s); defaulting to sync", exc)
        return True


def _parse_iso(ts_str):
    """Parse an ISO-8601 string (with or without Z / ms) to a UTC datetime."""
    ts_str = str(ts_str).strip().rstrip('Z')
    for fmt in ('%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse timestamp: {ts_str!r}")


# ---------------------------------------------------------------------------
# Firm sync (Step 5 in Workato recipe)
# ---------------------------------------------------------------------------
def sync_single_xero_firm_to_vp(instance):  # pylint: disable=too-many-locals,unused-argument
    """Sync one Xero contact to a VP Firm and upsert the map_firm row.

    Differs from mapping_sync's sync_xero_firms_to_vp in two ways:
    1. Reads from XCom task 'fetch_xero_contact' (singular, one contact).
    2. No anti-join guard — we upsert whether or not the contact was previously
       mapped; that's the point of the update path (Xero timestamp > ModDate).

    Called by `sync_firm_to_vp` PythonOperator in processor_dag.
    """
    import sqlite3  # pylint: disable=import-outside-toplevel
    import rail.lib.s3_collection  # pylint: disable=import-outside-toplevel
    from rail import (  # pylint: disable=import-outside-toplevel
        VantagepointFirmOperator,
        VantagepointFirmAddressOperator,
        S3UpsertCollectionOperator,
    )
    from vp_xero_integration_v2.mapping_sync.utils._firm_sync import (  # pylint: disable=import-outside-toplevel
        build_vp_firm_create_body,
        build_vp_firm_address_bodies,
        _parse_account_number,
        _load_vp_firms_by_name,
        _load_default_org,
        _load_codetable_index,
        _build_map_firm_row,
    )
    from vp_xero_integration_v2.mapping_sync.utils._shared import (  # pylint: disable=import-outside-toplevel
        _extract_xero_records,
        _extract_vp_client_id,
    )
    from vp_xero_integration_v2.common.python_callable_method import unwrap_vp_response  # pylint: disable=import-outside-toplevel
    from vp_xero_integration_v2.mapping_sync.config import IntegrationConfig  # pylint: disable=import-outside-toplevel

    context = rail.get_current_context()
    log = context['task_instance'].log

    contacts = _extract_xero_records(rail.result('fetch_xero_contact'))
    if not contacts:
        logger.warning("fetch_xero_contact returned no records; nothing to sync")
        return {'synced': 0}

    contact = contacts[0]
    contact_id = contact.get('ContactID')
    name = contact.get('Name') or ''
    log.info("Syncing Xero contact %s (%s) → VP Firm", contact_id, name)

    conn_ids = IntegrationConfig.get_conn_ids(context)
    vp_conn_id = conn_ids['vp_conn_id']

    s3_integration = IntegrationConfig.S3_INTEGRATION_NAME
    s3_customer = IntegrationConfig.get_s3_customer(context)
    # Pin to 'mapping_sync' — the map_firm collection is always owned by
    # mapping_sync, and processor_dag conf does not carry integrationType.
    s3_integration_type = MAPPING_COLLECTION_INTEGRATION_TYPE
    s3_artifact_name = rail.lib.s3_collection.get_s3_collection_artifact_name(
        context, s3_integration, s3_customer, s3_integration_type
    )

    mod_date = (
        datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    )

    client_code, vendor_code = _parse_account_number(contact.get('AccountNumber'))

    # Load create-time refs lazily (only needed for net-new firms)
    lazy = {
        'default_org': None, 'country_index': None,
        'state_index': None, 'loaded': False,
    }

    def _ensure_create_refs():
        if not lazy['loaded']:
            lazy['default_org'] = _load_default_org(vp_conn_id, context)
            lazy['country_index'] = _load_codetable_index(
                vp_conn_id, 'FW_CFGCountry', context)
            lazy['state_index'] = _load_codetable_index(
                vp_conn_id, 'CFGStates', context)
            lazy['loaded'] = True

    seen_addresses = set()

    def _create_firm_addresses(client_id):
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

    existing_map = rail.result('check_firm_map')
    if existing_map and existing_map.get('FirmID'):
        vp_firm = {'ClientID': existing_map['FirmID'], 'Name': name}
    else:
        vp_firm = _load_vp_firms_by_name(vp_conn_id, context).get(name.strip().lower())
    if vp_firm and vp_firm.get('ClientID'):
        client_id = vp_firm['ClientID']
        vantagepoint_name = vp_firm.get('Name') or name
        log.info("Updating existing VP firm '%s' (ClientID=%s)", name, client_id)
        _ensure_create_refs()
        update_body = build_vp_firm_create_body(
            contact, lazy['default_org'], client_code, vendor_code)
        VantagepointFirmOperator(
            task_id=f'_put_firm_{contact_id}',
            vp_conn_id=vp_conn_id,
            request_method='PUT',
            client_id=client_id,
            request_body=update_body,
            pagination=False,
        ).execute(context)
        _create_firm_addresses(client_id)
    else:
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
                f"VP firm create for contact {contact_id} ({name}) returned no ClientID"
            )
        vantagepoint_name = name
        log.info("Created new VP firm '%s' (ClientID=%s)", name, client_id)
        _create_firm_addresses(client_id)

    map_row = _build_map_firm_row(
        firm_id=client_id,
        contact_id=str(contact_id),
        status=contact.get('ContactStatus'),
        vendor=vendor_code,
        client=client_code,
        xero_name=name,
        vantagepoint_name=vantagepoint_name,
        mod_date=mod_date,
    )

    S3UpsertCollectionOperator(
        task_id='_upsert_map_firm_contact_update',
        integration=s3_integration,
        customer=s3_customer,
        integration_type=s3_integration_type,
        collection_name=MAP_FIRM_TABLE_NAME,
        key_columns=MAP_FIRM_UNIQUE_COLUMNS,
        rows=[map_row],
    ).execute(context)

    log.info("map_firm upserted for ContactID=%s ClientID=%s", contact_id, client_id)
    return {'synced': 1, 'ContactID': contact_id, 'ClientID': client_id}


# ---------------------------------------------------------------------------
# Error capture
# ---------------------------------------------------------------------------
def capture_processor_dag_error(error_message):
    """Catch-all error handler for processor_dag — mirrors map_firm_dag pattern.

    error_message is injected via op_args=['{{ get_error_message() }}'] so it
    contains the real exception text (context.get('exception') is empty for a
    task running via trigger_rule='one_failed').

    Returns an error dict (never raises) so the dispatcher's gather task can
    collect errors from all processor runs without short-circuiting.
    """
    context = rail.get_current_context()
    contact_id = context['dag_run'].conf.get('ContactID', 'unknown')
    msg = (error_message or '').strip() or '<no error message available>'
    error = {
        'ContactID': contact_id,
        'error': msg,
        'dag_run_id': context['dag_run'].run_id,
    }
    logger.error("processor_dag error for ContactID=%s: %s", contact_id, msg)
    return error

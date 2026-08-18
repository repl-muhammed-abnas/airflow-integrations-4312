"""
Shared Python callable methods for the PIM D365 integration DAGs.

Syncs 7 entities from Dynamics 365 to PIM (AERIS):
Lead, Opportunity, External Organisation, External Contact,
Internal Contact, Enquiry, Project.

All functions use rail.result() / rail.get_current_context() to read
upstream task outputs and dag_run.conf.  Nothing is imported from
config.py — values arrive via dag_run.conf or function parameters.
"""
# pylint: disable=invalid-name,broad-exception-caught
import json
import logging
from urllib.parse import quote

import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import rail

from associatedengineeringgroup.deltek_pim_d365_integration.config import (
    D365_API_VERSION,
    D365_ODATA_HEADERS,
    D365_TOKEN_VAR_PREFIX,
    MAPPING_TYPE_NAMES,
    PIM_CUSTOM_API,
    PIM_STANDARD_API_BASE,
    PIM_TOKEN_VAR_PREFIX,
)

log = logging.getLogger(__name__)


# ============================================================================
# Internal helpers
# ============================================================================

def _ctx():
    """Return the current Airflow context dict."""
    return rail.get_current_context()


def _conf():
    """Return dag_run.conf from the current context."""
    return _ctx()['dag_run'].conf


def _safe_str(value, default=''):
    """Return str(value) when value is not None, else *default*."""
    if value is None:
        return default
    return str(value)


def _format_date(iso_string):
    """Convert an ISO-8601 datetime string to YYYY-MM-DD or return None."""
    if not iso_string:
        return None
    return str(iso_string)[:10]


def _udf_obj(value):
    """Wrap a value as a UDF dropdown object ``{"name": value}``.

    PIM Custom APIs require dropdown UDF fields to be objects, not plain
    strings.  Returns *None* when *value* is falsy so the field can be
    omitted from the payload.
    """
    if not value:
        return None
    return {'name': str(value)}


# ============================================================================
# ExternalIntegrationMapping helpers
# ============================================================================

def validate_company_mapping():
    """IfOperator test — returns True when a valid Internal Org mapping exists."""
    mapping = rail.result('get_company_mapping')
    if not mapping or (isinstance(mapping, list) and len(mapping) == 0):
        return False
    return True


def check_mapping_exists():
    """IfOperator python_callable — returns *True* when a valid mapping
    with a destinationId exists for the entity.
    """
    mapping = rail.result('get_entity_mapping')
    if not mapping:
        return False
    if isinstance(mapping, list):
        mapping = mapping[0] if mapping else {}
    destination_id = mapping.get('destinationId') if isinstance(mapping, dict) else None
    return destination_id is not None and str(destination_id).strip() != ''


def extract_pim_id_from_mapping():
    """Extract the integer PIM entity ID from the mapping response."""
    mapping = rail.result('get_entity_mapping')
    if isinstance(mapping, list):
        mapping = mapping[0] if mapping else {}
    if not isinstance(mapping, dict):
        return None
    destination_id = mapping.get('destinationId')
    if destination_id is None:
        return None
    return int(destination_id)


def build_add_mapping_body(mapping_type_name):
    """Return a callable that builds the AddMapping request body.

    Parameters
    ----------
    mapping_type_name : str
        The ExternalIntegrationMapping type name
        (e.g. ``'Lead'``, ``'Opportunity'``).
    """
    def _inner():
        conf = _conf()
        source_guid = conf.get('entity_guid') or conf.get('entityGuid') or conf.get('sourceGuid')
        if not source_guid:
            raise ValueError("No source GUID found in dag_run.conf")

        create_response = rail.result('create_entity')
        if isinstance(create_response, dict):
            destination_id = (
                create_response.get('id')
                or create_response.get('Id')
                or create_response.get('entityId')
            )
        else:
            destination_id = create_response
        if not destination_id:
            raise ValueError(f"No destination ID from create_entity: {create_response}")

        return json.dumps({
            'sourceGuid': str(source_guid),
            'destinationId': int(destination_id),
        })
    return _inner


def check_if_field_is_truthy(d365_entity, field_name):
    """Return whether a field on a D365 entity dict is truthy.

    Parameters
    ----------
    d365_entity : dict
        The resolved D365 entity record.
    field_name : str
        The field name to check (e.g. ``_ae_company_value``).
    """
    value = d365_entity.get(field_name)
    return bool(value)


# ============================================================================
# 6-7  D365 response helpers
# ============================================================================

def extract_d365_entity(task_id):
    """Return a callable that extracts the entity record from a D365
    OData response.

    Handles both single-entity GET (the dict itself) and collection
    responses (``value`` array — takes the first element).

    Parameters
    ----------
    task_id : str
        The upstream task whose result contains the D365 response.
    """
    def _inner():
        response = rail.result(task_id)
        if isinstance(response, dict):
            if 'value' in response:
                records = response['value']
                return records[0] if records else {}
            return response
        return response
    return _inner


def resolve_lookup_field(entity, field_name):
    """Extract the ``_fieldname_value`` lookup GUID from a D365 entity.

    D365 exposes navigation-property foreign keys as
    ``_<fieldname>_value``.  This helper normalises the lookup.

    Parameters
    ----------
    entity : dict
        The D365 entity record.
    field_name : str
        The logical field name (without leading underscore / trailing
        ``_value``).

    Returns
    -------
    str or None
        The GUID string, or *None* when the lookup is empty.
    """
    if not entity or not isinstance(entity, dict):
        return None
    lookup_key = f'_{field_name}_value'
    value = entity.get(lookup_key)
    if value:
        return str(value)
    # Fallback: try the field name as-is (already expanded)
    return entity.get(field_name)


# ============================================================================
# 8  Lead body builder
# ============================================================================

def build_lead_body(operation):
    """Return a callable that maps D365 Lead fields to PIM Lead.ashx.

    Division/office/group/company are resolved via ExternalIntegrationMapping
    lookups (from the lead's vs360_segment).  Status uses statecode
    (0=Active, 1=Inactive), resolved to a PIM status ID via GetLeadStatuses.

    Parameters
    ----------
    operation : str
        ``'create'`` or ``'update'``.
    """
    def _inner():
        entity = rail.result('get_d365_entity')
        if isinstance(entity, dict) and 'value' in entity:
            entity = entity['value'][0] if entity['value'] else {}

        statecode = entity.get('statecode', 0)

        body = {
            'code': _safe_str(entity.get('ae_leadid')),
            'name': _safe_str(entity.get('vs360_name')),
            'confidential': bool(entity.get('vs360_confidential')),
            'startDate': entity.get('vs360_estimatedstartdate'),
            'description': _safe_str(entity.get('vs360_scope')),
            'value': entity.get('vs360_value'),
        }

        # Status: D365 Active (0) → PIM ID 1, D365 Inactive (1) → PIM ID 4
        body['status'] = {'id': 4 if statecode == 1 else 1}

        # Division / Office / Group — resolved via process_udfs child DAG
        div_id = _get_udf_id('get_pim_division_id')
        if div_id is not None:
            body['division'] = {'id': div_id}

        office_id = _get_udf_id('get_pim_office_id')
        if office_id is not None:
            body['office'] = {'id': office_id}

        group_id = _get_udf_id('get_pim_group_id')
        if group_id is not None:
            body['group'] = {'id': group_id}

        # Company — read-only mapping, never created inline
        company_id = _get_mapped_id('get_company_mapping')
        if company_id is not None:
            body['company'] = {'id': company_id}

        # Organisation — from primary client mapping or post-sync mapping
        org_id = None
        try:
            client_mapping = rail.result('get_primary_client_mapping')
            if isinstance(client_mapping, list):
                client_mapping = client_mapping[0] if client_mapping else {}
            if isinstance(client_mapping, dict) and client_mapping.get('destinationId'):
                org_id = int(client_mapping['destinationId'])
        except Exception:
            pass

        if org_id is None:
            try:
                post_sync = rail.result('fetch_org_mapping_after_sync')
                if isinstance(post_sync, list):
                    post_sync = post_sync[0] if post_sync else {}
                if isinstance(post_sync, dict) and post_sync.get('destinationId'):
                    org_id = int(post_sync['destinationId'])
            except Exception:
                pass

        if org_id is not None:
            body['organisation'] = {'id': org_id}

        if operation == 'update':
            pim_id = extract_pim_id_from_mapping()
            if pim_id is not None:
                body['id'] = pim_id

        body = {k: v for k, v in body.items() if v is not None}
        return json.dumps(body)
    return _inner


# ============================================================================
# 9  Opportunity body builder
# ============================================================================

def build_opportunity_body(operation):
    """Return a callable that maps D365 Opportunity to PIM
    Opportunity.ashx.

    Division/office/group/company are resolved via ExternalIntegrationMapping
    lookups (from the opportunity's vs360_segment).  Status uses statecode
    (0=Active → PIM ID 1, 1=Inactive → PIM ID 4).

    Parameters
    ----------
    operation : str
        ``'create'`` or ``'update'``.
    """
    def _inner():
        entity = rail.result('get_d365_entity')
        if isinstance(entity, dict) and 'value' in entity:
            entity = entity['value'][0] if entity['value'] else {}

        statecode = entity.get('statecode', 0)

        body = {
            'code': _safe_str(entity.get('vs360_opportunitynumber')),
            'name': _safe_str(entity.get('name')),
            'confidential': bool(entity.get('vs360_confidential')),
            'description': _safe_str(entity.get('vs360_scope')),
            'value': entity.get('vs360_totalprojectvalue'),
        }

        # Status: D365 Active (0) → PIM ID 1, D365 Inactive (1) → PIM ID 4
        body['status'] = {'id': 4 if statecode == 1 else 1}

        # Division / Office / Group — resolved via process_udfs child DAG
        div_id = _get_udf_id('get_pim_division_id')
        if div_id is not None:
            body['division'] = {'id': div_id}

        office_id = _get_udf_id('get_pim_office_id')
        if office_id is not None:
            body['office'] = {'id': office_id}

        group_id = _get_udf_id('get_pim_group_id')
        if group_id is not None:
            body['group'] = {'id': group_id}

        # Company — read-only mapping, never created inline
        company_id = _get_mapped_id('get_company_mapping')
        if company_id is not None:
            body['company'] = {'id': company_id}

        # Organisation — from primary client mapping or post-sync mapping
        org_id = None
        try:
            client_mapping = rail.result('get_primary_client_mapping')
            if isinstance(client_mapping, list):
                client_mapping = client_mapping[0] if client_mapping else {}
            if isinstance(client_mapping, dict) and client_mapping.get('destinationId'):
                org_id = int(client_mapping['destinationId'])
        except Exception:
            pass

        if org_id is None:
            try:
                post_sync = rail.result('fetch_org_mapping_after_sync')
                if isinstance(post_sync, list):
                    post_sync = post_sync[0] if post_sync else {}
                if isinstance(post_sync, dict) and post_sync.get('destinationId'):
                    org_id = int(post_sync['destinationId'])
            except Exception:
                pass

        if org_id is not None:
            body['organisation'] = {'id': org_id}

        if operation == 'update':
            pim_id = extract_pim_id_from_mapping()
            if pim_id is not None:
                body['id'] = pim_id

        body = {k: v for k, v in body.items() if v is not None}
        return json.dumps(body)
    return _inner


# ============================================================================
# 10  External Organisation body builder
# ============================================================================

def build_external_org_body(operation):
    """Return a callable that maps D365 Account to PIM Standard API
    ``/organisations``.

    Parameters
    ----------
    operation : str
        ``'create'`` or ``'update'``.
    """
    def _inner():
        entity = rail.result('get_d365_entity')
        if isinstance(entity, dict) and 'value' in entity:
            entity = entity['value'][0] if entity['value'] else {}

        body = {
            'name': _safe_str(entity.get('name')),
        }

        # alternativeNames — PIM expects a plain string, not an array
        former_name = entity.get('ae_formerlyknownas')
        if former_name:
            body['alternativeNames'] = _safe_str(former_name)

        # Strip None values — PIM rejects null fields
        body = {k: v for k, v in body.items() if v is not None}

        # Status mapping: Active(0) -> active status, Inactive(1) -> no longer trading
        statecode = entity.get('statecode')
        if statecode == 0:
            body['status'] = {'id': 1}
        elif statecode == 1:
            body['noLongerTradingReasonId'] = 5

        if operation == 'update':
            pim_id = extract_pim_id_from_mapping()
            if pim_id is not None:
                body['id'] = pim_id

        return json.dumps(body)
    return _inner


# ============================================================================
# 11  External Contact body builder
# ============================================================================

def build_external_contact_body(operation):
    """Return a callable that maps D365 Contact to PIM Standard API
    ``/contacts``.

    Parameters
    ----------
    operation : str
        ``'create'`` or ``'update'``.
    """
    def _inner():
        entity = rail.result('get_d365_entity')
        if isinstance(entity, dict) and 'value' in entity:
            entity = entity['value'][0] if entity['value'] else {}

        body = {
            'forename': _safe_str(entity.get('firstname')),
            'surname': _safe_str(entity.get('lastname')),
        }
        if entity.get('emailaddress1'):
            body['emailAddress'] = str(entity['emailaddress1'])
        if entity.get('jobtitle'):
            body['jobTitle'] = entity.get('jobtitle')
        # Organization via parentaccountid mapping
        org_mapping = rail.result('fetch_organization_mapping') or rail.result('fetch_organization_mapping2')
        if isinstance(org_mapping, list):
            org_mapping = org_mapping[0] if org_mapping else {}
        if isinstance(org_mapping, dict) and org_mapping.get('destinationId'):
            body['organization'] = {'id': int(org_mapping['destinationId'])}

        if operation == 'update':
            pim_id = extract_pim_id_from_mapping()
            if pim_id is not None:
                body['id'] = pim_id

        return json.dumps(body)
    return _inner


# ============================================================================
# 12  Internal Contact body builder
# ============================================================================

def build_internal_contact_body(operation):
    """Return a callable that maps D365 vs360_employee to PIM
    ``/contacts``.

    Internal contacts use a **negative** organisation ID derived from
    the Company mapping via ``ae_groupprofitcentre.ae_company``.

    Parameters
    ----------
    operation : str
        ``'create'`` or ``'update'``.
    """
    def _inner():
        entity = rail.result('get_d365_entity')
        if isinstance(entity, dict) and 'value' in entity:
            entity = entity['value'][0] if entity['value'] else {}

        forename = entity.get('vs360_knownasname') or entity.get('vs360_firstname')

        body = {
            'forename': _safe_str(forename),
            'surname': _safe_str(entity.get('vs360_lastname')),
        }
        if entity.get('emailaddress'):
            body['emailAddress'] = str(entity['emailaddress'])

        # Organisation: destinationId from Internal Org mapping (validated by check_company_mapping)
        company_mapping = rail.result('get_company_mapping')
        if isinstance(company_mapping, list):
            company_mapping = company_mapping[0] if company_mapping else {}
        destination_id = company_mapping.get('destinationId') if isinstance(company_mapping, dict) else None
        if destination_id:
            body['organization'] = {'id': int(destination_id)}

        # Status mapping: statecode 0 -> active, 1 -> set Superceded_By_Date
        statecode = entity.get('statecode')
        if statecode == 1:
            body['Superceded_By_Date'] = _format_date(entity.get('vs360_terminationdate'))

        if operation == 'update':
            pim_id = extract_pim_id_from_mapping()
            if pim_id is not None:
                body['id'] = pim_id

        return json.dumps(body)
    return _inner


# ============================================================================
# 13  Enquiry body builder
# ============================================================================

_ENQUIRY_STATUS_MAP = {
    # D365 StatusReason / Status label -> PIM Enquiry status id
    # Confirmed from GET DropdownValues.ashx?function=GetEnquiryStatuses
    'In Process': 3,
    'Active': 3,
    'Won': 4,
    'Lost': 5,
    'Cancelled by Client': 6,
    'Canceled by Client': 6,
    'Abandoned': 8,
}

_ENQUIRY_STATUS_DEFAULT = 3  # Active — fallback when D365 label has no mapping yet


def build_enquiry_body(operation):
    """Return a callable that maps D365 OpportunityProduct to PIM
    Standard API ``/enquiries``.

    Parameters
    ----------
    operation : str
        ``'create'`` or ``'update'``.
    """
    def _inner():
        entity = rail.result('get_d365_entity')
        if isinstance(entity, dict) and 'value' in entity:
            entity = entity['value'][0] if entity['value'] else {}

        # ae_statusreason takes priority over vs360_status (per tech spec)
        status_reason = entity.get('ae_statusreason@OData.Community.Display.V1.FormattedValue')
        state_code = entity.get('vs360_status@OData.Community.Display.V1.FormattedValue')
        raw_status = status_reason or state_code or ''
        status_id = _ENQUIRY_STATUS_MAP.get(raw_status)
        if status_id is None:
            log.warning(
                'Unmapped D365 enquiry status %r — defaulting to Active (%s)',
                raw_status, _ENQUIRY_STATUS_DEFAULT,
            )
            status_id = _ENQUIRY_STATUS_DEFAULT

        body = {
            'name': _safe_str(entity.get('productdescription')),
            'status': {'id': status_id},
            'plannedToStartOn': _format_date(entity.get('vs360_scheduledstart')),
            'description': _safe_str(entity.get('ae_descriptionscope')),
        }

        if operation == 'create':
            body['code'] = _safe_str(entity.get('ae_opportunitylineid'))
        else:
            pim_id = extract_pim_id_from_mapping()
            if pim_id is not None:
                body['id'] = int(pim_id)

        # Strip None values — PIM rejects null fields (DateOnly, Int32 cannot be null)
        body = {k: v for k, v in body.items() if v is not None}
        return json.dumps(body)
    return _inner


# ============================================================================
# 14  Enquiry UDF body builder
# ============================================================================

def build_enquiry_udf_body():
    """Return a callable that builds the Enquiry.ashx UpdateEnquiryUDF
    payload.

    Division/office/group are resolved via ExternalIntegrationMapping IDs
    (created by upstream DropdownValues.ashx tasks if missing).
    Company is read-only — omitted if unmapped.
    Confidential is inherited from the parent opportunity.
    """
    def _inner():
        entity = rail.result('get_d365_entity')
        if isinstance(entity, dict) and 'value' in entity:
            entity = entity['value'][0] if entity['value'] else {}

        pim_id = extract_pim_id_from_mapping()
        if pim_id is None:
            create_result = rail.result('create_entity')
            if create_result is not None:
                pim_id = int(create_result)
        if pim_id is None:
            raise ValueError(
                'Cannot build Enquiry UDF body: no PIM enquiry id resolved '
                'from mapping or create_entity result.'
            )

        # Division/office/group: resolved via process_udfs child DAG
        div_id = _get_udf_id('get_pim_division_id')
        office_id = _get_udf_id('get_pim_office_id')
        group_id = _get_udf_id('get_pim_group_id')

        # Company: read-only mapping — omitted if unmapped (never created)
        company_id = _get_mapped_id('get_company_mapping')

        # Confidential inherited from parent opportunity
        parent_opp = entity.get('opportunityid') or {}
        confidential = bool(parent_opp.get('vs360_confidential'))

        body = {'id': pim_id, 'confidential': confidential}

        if div_id is not None:
            body['division'] = {'id': div_id}
        if office_id is not None:
            body['office'] = {'id': office_id}
        if group_id is not None:
            body['group'] = {'id': group_id}
        if company_id is not None:
            body['company'] = {'id': company_id}

        capital_value = entity.get('ae_capitalvalueofproject')
        if capital_value is not None:
            body['value'] = capital_value

        return json.dumps(body)
    return _inner


# ============================================================================
# 15  Project body builder
# ============================================================================


# ============================================================================
# 16  Entity Contacts body builder
# ============================================================================

def build_entity_contacts_body(class_id):
    """Return a callable that builds an EntityContacts.ashx payload.

    Parameters
    ----------
    class_id : int
        The PIM class ID for the entity type (e.g. Lead, Opportunity).
    """
    def _inner():
        pim_id = extract_pim_id_from_mapping()

        contacts_data = rail.result('get_entity_contacts')
        if not contacts_data:
            contacts_data = []
        if isinstance(contacts_data, dict):
            contacts_data = contacts_data.get('value', [contacts_data])

        contact_list = []
        for contact in contacts_data:
            contact_guid = contact.get('contactGuid') or contact.get('sourceGuid')
            if not contact_guid:
                continue

            contact_mapping = contact.get('mapping') or {}
            contact_pim_id = contact_mapping.get('destinationId')

            if contact_pim_id is not None:
                entry = {
                    'id': int(contact_pim_id),
                    'primaryContact': bool(contact.get('primaryContact')),
                    'projectManager': bool(contact.get('projectManager')),
                }
                contact_list.append(entry)

        body = {
            'id': pim_id,
            'classId': class_id,
            'contact': contact_list,
        }
        return json.dumps(body)
    return _inner


# ============================================================================
# 18  Division / Office / Group resolver
# ============================================================================

def resolve_division_office_group(segment_data):
    """Extract division, office and group from an expanded
    ``vs360_segment`` navigation property as UDF objects.

    Parameters
    ----------
    segment_data : dict or None
        The expanded segment entity from D365.

    Returns
    -------
    tuple[dict|None, dict|None, dict|None]
        ``(division, office, group)`` as ``{"name": "..."}`` objects,
        or *None* when absent.
    """
    if not segment_data or not isinstance(segment_data, dict):
        return (None, None, None)

    division_name = segment_data.get(
        '_vs360_marketid_value@OData.Community.Display.V1.FormattedValue',
        _safe_str(segment_data.get('_vs360_marketid_value', '')),
    )
    office_name = segment_data.get(
        '_ae_office_value@OData.Community.Display.V1.FormattedValue',
        _safe_str(segment_data.get('_ae_office_value', '')),
    )
    group_name = _safe_str(segment_data.get('vs360_name', ''))

    return (_udf_obj(division_name), _udf_obj(office_name), _udf_obj(group_name))


# ============================================================================
# 19-21  Response filter helpers
# ============================================================================

def filter_mapping_by_type(type_name):
    """Return a response_filter that picks only the mapping with the given type name."""
    def _filter(response):
        data = safe_json_response(response)
        if isinstance(data, list):
            return [m for m in data if m.get('type', {}).get('name') == type_name]
        return data
    return _filter


def safe_json_response(response):
    """response_filter that handles empty API responses gracefully.

    PIM's GetMapping endpoint returns an empty body (no JSON) when no
    mapping exists.  This filter returns an empty list in that case so
    downstream ``check_mapping_exists`` works correctly.
    """
    if not response.content or not response.text.strip():
        return []
    try:
        return response.json()
    except Exception:
        log.warning(
            'GetMapping returned non-JSON response (status=%s): %s',
            response.status_code,
            response.text[:200],
        )
        return []


def extract_id_from_response_data(data, key='id', type_name=None):
    """Extract an ID field from pre-parsed JSON response data.

    Works on the output already stored in XCom by a SimpleHttpOperator
    after response_filter has been separated into its own task.

    When type_name is provided the list is filtered to the entry whose
    type.name matches before the first element is picked.  This is needed
    because PIM's GetMapping endpoint ignores the &name= query parameter
    server-side and returns all mappings for the given sourceGuid; the
    caller must filter client-side.
    """
    if not data:
        return None
    if isinstance(data, list):
        if type_name:
            data = [item for item in data if item.get('type', {}).get('name') == type_name]
        return data[0].get(key) if data else None
    if isinstance(data, dict):
        return data.get(key)
    return None


def extract_token(response):
    """response_filter for OAuth token requests.

    Returns the ``access_token`` value from the JSON response.
    """
    return response.json()['access_token']


def extract_pim_entity_id(response):
    """response_filter for PIM Standard API create responses.

    Extracts the newly created entity ID from the response.
    """
    data = response.json()
    if isinstance(data, dict):
        return data.get('id') or data.get('Id')
    return data


def extract_custom_api_id(response):
    """response_filter for PIM Custom API (Lead.ashx / Opportunity.ashx)
    create responses.

    Extracts the entity ID from the custom API response format.
    Handles empty responses and XML error responses from .ashx endpoints.
    """
    log.info(
        'Custom API response: status=%s, headers=%s, body=%r',
        response.status_code,
        dict(response.headers),
        response.text[:1000] if response.text else '(empty)',
    )
    if not response.content or not response.text.strip():
        log.warning('Custom API returned empty response (status=%s)', response.status_code)
        return None
    raw = response.text.strip()
    if raw.startswith('<error') or raw.startswith('<?xml'):
        raise ValueError(f'Custom API returned error: {raw[:500]}')
    try:
        data = response.json()
    except Exception:
        if raw.isdigit():
            log.info('Custom API returned plain integer ID: %s', raw)
            return int(raw)
        raise ValueError(f'Custom API returned unexpected response: {raw[:500]}')
    if isinstance(data, dict):
        entity_id = (
            data.get('id')
            or data.get('Id')
            or data.get('entityId')
            or data.get('EntityId')
        )
        log.info('Custom API returned dict, extracted entity_id=%s from keys=%s', entity_id, list(data.keys()))
        return entity_id
    log.info('Custom API returned non-dict value: %r (type=%s)', data, type(data).__name__)
    return data


# ============================================================================
# 22  Reference data (division/office/group) helpers
# ============================================================================

def validate_ref_data_mapping(task_id):
    """Check whether a reference-data mapping (division/office/group) exists.

    Returns True when the mapping result has a valid destinationId.
    Used inside a lambda for IfOperator test callables.
    """
    mapping = rail.result(task_id)
    if not mapping or (isinstance(mapping, list) and len(mapping) == 0):
        return False
    if isinstance(mapping, list):
        mapping = mapping[0]
    return mapping.get('destinationId') is not None


def check_entity_field(field_name):
    """Return a callable that checks if a field on the D365 entity is non-null."""
    def _inner():
        entity = rail.result('get_d365_entity')
        return bool(entity.get(field_name))
    return _inner


def check_segment_field(field_name):
    """Return a callable that checks if a field on the D365 segment is non-null."""
    def _inner():
        segment = rail.result('fetch_d365_segment')
        if not segment or not isinstance(segment, dict):
            return False
        return bool(segment.get(field_name))
    return _inner


def build_merged_ref_data():
    """Build unified ref-data source from segment (preferred) or direct entity fields.

    When a segment was fetched, returns it as-is.  When no segment exists,
    maps the direct opportunity fields to segment-compatible keys so the
    downstream division/office/group/company chains work unchanged.
    """
    def _inner():
        try:
            segment = rail.result('fetch_d365_segment')
            if segment and isinstance(segment, dict):
                return segment
        except Exception:
            pass

        entity = rail.result('get_d365_entity')
        if not entity or not isinstance(entity, dict):
            return {}

        fv = '@OData.Community.Display.V1.FormattedValue'
        return {
            '_vs360_marketid_value': entity.get('_vs360_marketid_value'),
            f'_vs360_marketid_value{fv}': entity.get(
                f'_vs360_marketid_value{fv}', ''),
            '_ae_office_value': entity.get('_vs360_officeid_value'),
            f'_ae_office_value{fv}': entity.get(
                f'_vs360_officeid_value{fv}', ''),
            '_ae_company_value': entity.get('_vs360_company_value'),
            f'_ae_company_value{fv}': entity.get(
                f'_vs360_company_value{fv}', ''),
            'vs360_name': entity.get(
                f'_vs360_segmentid_value{fv}', ''),
            'vs360_segmentid': entity.get('_vs360_segmentid_value'),
        }
    return _inner


def _get_mapped_id(task_id):
    """Extract destinationId from a mapping task result. Returns int or None."""
    try:
        mapping = rail.result(task_id)
    except Exception:
        return None
    if isinstance(mapping, list):
        mapping = mapping[0] if mapping else {}
    if isinstance(mapping, dict) and mapping.get('destinationId'):
        return int(mapping['destinationId'])
    return None


def _get_ref_data_id(mapping_task_id, create_task_id):
    """Get PIM ID from an existing mapping, falling back to a newly created entity.

    Used for division/office/group where the ref data may have been created
    in the same DAG run — ``get_*_mapping`` returns empty but
    ``create_*`` holds the new PIM ID.
    """
    mapped = _get_mapped_id(mapping_task_id)
    if mapped is not None:
        return mapped
    try:
        create_result = rail.result(create_task_id)
        if isinstance(create_result, dict):
            val = create_result.get('id') or create_result.get('Id') or create_result.get('entityId')
            if val is not None:
                return int(val)
        elif create_result is not None:
            return int(create_result)
    except Exception:
        pass
    return None


def _get_udf_id(task_id):
    """Extract PIM ID from a GatherResultsFromDagRunsOperator result.

    Used when division/office/group are resolved via the process_udfs
    child DAG instead of inline resolve-or-create chains.
    """
    try:
        result = rail.result(task_id)
        if isinstance(result, list):
            return int(result[0]) if result else None
        if result is not None:
            return int(result)
    except Exception:
        pass
    return None


def build_ref_data_body(entity_task_id, formatted_value_field):
    """Return a callable that builds ``{"name": "..."}`` for DropdownValues.ashx.

    Parameters
    ----------
    entity_task_id : str
        Task ID whose result contains the D365 entity.
    formatted_value_field : str
        The OData FormattedValue annotation key (e.g.
        ``'_vs360_marketid_value@OData.Community.Display.V1.FormattedValue'``).
    """
    def _inner():
        entity = rail.result(entity_task_id)
        if isinstance(entity, dict) and 'value' in entity:
            entity = entity['value'][0] if entity['value'] else {}
        name = entity.get(formatted_value_field, '')
        return json.dumps({'name': str(name)})
    return _inner


def build_ref_data_mapping_body(entity_task_id, guid_field, create_task_id):
    """Return a callable that builds AddMapping body after creating ref data.

    Parameters
    ----------
    entity_task_id : str
        Task ID whose result contains the D365 entity (source GUID).
    guid_field : str
        The lookup field key on the entity (e.g. ``'_vs360_marketid_value'``).
    create_task_id : str
        Task ID whose result contains the newly created PIM dropdown ID.
    """
    def _inner():
        entity = rail.result(entity_task_id)
        if isinstance(entity, dict) and 'value' in entity:
            entity = entity['value'][0] if entity['value'] else {}
        source_guid = entity.get(guid_field)
        if not source_guid:
            raise ValueError(f"No source GUID found in field '{guid_field}'")

        create_result = rail.result(create_task_id)
        if isinstance(create_result, dict):
            destination_id = (
                create_result.get('id')
                or create_result.get('Id')
                or create_result.get('entityId')
            )
        else:
            destination_id = create_result
        if not destination_id:
            raise ValueError(f"No destination ID from {create_task_id}: {create_result}")

        return json.dumps({
            'sourceGuid': str(source_guid),
            'destinationId': int(destination_id),
        })
    return _inner


# ============================================================================
# 23  Inline external org creation (testing path)
# ============================================================================

def build_inline_org_body(fetch_account_task_id):
    """Return a callable that builds a minimal org create body from a D365 Account fetch.

    Used as a testing shortcut — production flow should trigger the
    external_org_sync DAG instead.
    """
    def _inner():
        account = rail.result(fetch_account_task_id)
        if isinstance(account, dict) and 'value' in account:
            account = account['value'][0] if account['value'] else {}
        return json.dumps({
            'name': _safe_str(account.get('name')),
        })
    return _inner


def build_inline_org_mapping_body(entity_task_id, guid_field, create_task_id):
    """Return a callable that builds AddMapping body for an inline-created org.

    Same as ``build_ref_data_mapping_body`` but extracts the PIM org ID
    from a Standard API create response (``id`` or ``Id``).
    """
    def _inner():
        entity = rail.result(entity_task_id)
        if isinstance(entity, dict) and 'value' in entity:
            entity = entity['value'][0] if entity['value'] else {}
        source_guid = entity.get(guid_field)
        if not source_guid:
            raise ValueError(f"No source GUID found in field '{guid_field}'")

        create_result = rail.result(create_task_id)
        if isinstance(create_result, dict):
            destination_id = create_result.get('id') or create_result.get('Id')
        else:
            destination_id = create_result
        if not destination_id:
            raise ValueError(f"No destination ID from {create_task_id}: {create_result}")

        return json.dumps({
            'sourceGuid': str(source_guid),
            'destinationId': int(destination_id),
        })
    return _inner


# ============================================================================
# 24  Lead contacts resolver (inline external contact creation)
# ============================================================================

def _pim_api_call(pim_conn_id, instance, method, path, body=None):
    """Make a PIM API call using Airflow connection + Variable token."""
    conn = BaseHook.get_connection(pim_conn_id)
    base_url = f"https://{conn.host}" if conn.host and not conn.host.startswith('http') else (conn.host or '')
    base_url = base_url.rstrip('/')
    token = Variable.get(f'{PIM_TOKEN_VAR_PREFIX}_{instance}')
    headers = {'Authorization': f'Bearer {token}'}
    if method == 'POST':
        headers['Content-Type'] = 'application/json'
    url = f'{base_url}{path}'
    resp = requests.request(method, url, headers=headers, json=body if method == 'POST' else None, timeout=30)
    if not resp.ok:
        msg = f'PIM API {method} {path} returned {resp.status_code}: {resp.text[:500]}'
        log.error(msg)
        raise RuntimeError(msg)
    if resp.content and resp.text.strip():
        return resp.json()
    return None


def _d365_api_call(d365_conn_id, token, path):
    """Make a D365 OData GET call using Airflow connection."""
    conn = BaseHook.get_connection(d365_conn_id)
    base_url = f"https://{conn.host}" if conn.host and not conn.host.startswith('http') else (conn.host or '')
    base_url = base_url.rstrip('/')
    headers = {
        'Authorization': f'Bearer {token}',
        **D365_ODATA_HEADERS,
    }
    url = f'{base_url}{path}'
    resp = requests.get(url, headers=headers, timeout=30)
    if not resp.ok:
        msg = f'D365 API GET {path} returned {resp.status_code}: {resp.text[:500]}'
        log.error(msg)
        raise RuntimeError(msg)
    return resp.json()


def _get_pim_mapping(pim_conn_id, instance, mapping_type_name, source_guid):
    """Look up an ExternalIntegrationMapping by type and source GUID."""
    path = (
        f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
        f"?function=GetMapping"
        f"&name={quote(mapping_type_name)}"
        f"&source={source_guid}"
    )
    result = _pim_api_call(pim_conn_id, instance, 'GET', path)
    if not result:
        return None
    if isinstance(result, list):
        return result[0] if result else None
    return result


def _create_pim_mapping(pim_conn_id, instance, mapping_type_name, source_guid, destination_id):
    """Create an ExternalIntegrationMapping."""
    path = (
        f"/XWeb/CustomAPI/{PIM_CUSTOM_API['EXTERNAL_INTEGRATION_MAPPING']}"
        f"?function=AddMapping&name={quote(mapping_type_name)}"
    )
    body = {'sourceGuid': str(source_guid), 'destinationId': int(destination_id)}
    return _pim_api_call(pim_conn_id, instance, 'POST', path, body)


def _resolve_external_contact(pim_conn_id, d365_conn_id, instance, d365_token, contact_guid):
    """Resolve a single external contact GUID to a PIM Contact ID.

    Lookup-only — returns the mapped PIM ID or None.
    External contacts are pre-synced via external_contact_sync DAG
    before this function is called.
    """
    mapping = _get_pim_mapping(
        pim_conn_id, instance,
        MAPPING_TYPE_NAMES['EXTERNAL_CONTACT'],
        contact_guid,
    )
    if mapping and mapping.get('destinationId'):
        return int(mapping['destinationId'])
    return None


def resolve_lead_contacts(pim_conn_id, d365_conn_id, instance):
    """Return a callable that resolves lead contacts with PIM ID lookups.

    Builds the full contact list for EntityContacts.ashx SyncList:
    1. Primary contact (``vs360_contactid`` on lead) — ExternalContact,
       ``primaryContact = true``.  Always included even if not in
       ``vs360_jobleadcontact``.
    2. All contacts from ``vs360_jobleadcontact`` — ExternalContact.
    3. Proposal manager (``ae_proposalmanager2``) — InternalContact,
       ``projectManager = true``.  Resolved via upstream operator tasks
       (``get_pm_mapping`` / ``get_pm_mapping_after_sync``).

    Unmapped external contacts are created inline (testing path).
    """
    def _inner():
        entity = rail.result('get_d365_entity')
        if not entity:
            return []
        primary_guid = entity.get('_vs360_contactid_value')
        d365_token = Variable.get(f'{D365_TOKEN_VAR_PREFIX}_{instance}')

        seen_guids = set()
        resolved = []

        # 1. Primary contact from lead entity (always include)
        if primary_guid:
            pim_id = _resolve_external_contact(
                pim_conn_id, d365_conn_id, instance, d365_token, primary_guid,
            )
            if pim_id:
                resolved.append({
                    'id': pim_id,
                    'primaryContact': True,
                    'projectManager': False,
                })
                seen_guids.add(str(primary_guid))

        # 2. Contacts from vs360_jobleadcontact
        contacts_raw = rail.result('fetch_d365_lead_contacts') or []
        for record in contacts_raw:
            contact_guid = record.get('_vs360_contactid_value')
            if not contact_guid or str(contact_guid) in seen_guids:
                continue

            pim_id = _resolve_external_contact(
                pim_conn_id, d365_conn_id, instance, d365_token, contact_guid,
            )
            if pim_id:
                resolved.append({
                    'id': pim_id,
                    'primaryContact': False,
                    'projectManager': False,
                })
                seen_guids.add(str(contact_guid))

        # 3. Proposal manager from upstream InternalContact mapping
        pm_mapping = None
        for task_id in ('get_pm_mapping_after_sync', 'get_pm_mapping'):
            try:
                result = rail.result(task_id)
                if result:
                    pm_mapping = result
                    break
            except Exception:
                continue

        if isinstance(pm_mapping, list):
            pm_mapping = pm_mapping[0] if pm_mapping else {}
        if isinstance(pm_mapping, dict) and pm_mapping.get('destinationId'):
            pm_pim_id = int(pm_mapping['destinationId'])
            existing = [c for c in resolved if c['id'] == pm_pim_id]
            if existing:
                existing[0]['projectManager'] = True
            else:
                resolved.append({
                    'id': pm_pim_id,
                    'primaryContact': False,
                    'projectManager': True,
                })

        return resolved
    return _inner


def resolve_opportunity_contacts(pim_conn_id, d365_conn_id, instance):
    """Return a callable that resolves opportunity contacts with PIM ID lookups.

    Builds the full contact list for EntityContacts.ashx:
    1. All contacts from ``vs360_opportunitycontact`` — ExternalContact.
       (The opportunity entity itself has no primary contact field;
       primary contact is not distinguished at the opportunity level.)
    2. Team members from ``vs360_opportunityteam`` — InternalContact,
       resolved via INTERNAL_CONTACT mapping.
    3. Proposal manager (``ae_proposalmanager2``) — InternalContact,
       ``projectManager = true``.  Resolved via upstream operator tasks.
    """
    def _inner():
        entity = rail.result('get_d365_entity')
        if not entity:
            return []
        d365_token = Variable.get(f'{D365_TOKEN_VAR_PREFIX}_{instance}')

        seen_guids = set()
        resolved = []

        # 1. Contacts from vs360_opportunitycontact
        contacts_raw = rail.result('fetch_opp_contacts') or []
        for record in contacts_raw:
            contact_guid = record.get('_vs360_contactid_value')
            if not contact_guid or str(contact_guid) in seen_guids:
                continue

            pim_id = _resolve_external_contact(
                pim_conn_id, d365_conn_id, instance, d365_token, contact_guid,
            )
            if pim_id:
                resolved.append({
                    'id': pim_id,
                    'primaryContact': False,
                    'projectManager': False,
                })
                seen_guids.add(str(contact_guid))

        # 3. Team members (internal contacts) — resolve via INTERNAL_CONTACT mapping
        team_members = rail.result('fetch_opp_team') or []
        for record in team_members:
            employee_guid = record.get('_vs360_employeeid_value')
            if not employee_guid or str(employee_guid) in seen_guids:
                continue

            mapping = _get_pim_mapping(
                pim_conn_id, instance,
                MAPPING_TYPE_NAMES['INTERNAL_CONTACT'], str(employee_guid),
            )
            if mapping and mapping.get('destinationId'):
                resolved.append({
                    'id': int(mapping['destinationId']),
                    'primaryContact': False,
                    'projectManager': False,
                })
                seen_guids.add(str(employee_guid))

        # 4. Proposal manager from upstream InternalContact mapping
        pm_mapping = None
        for task_id in ('get_pm_mapping_after_sync', 'get_pm_mapping'):
            try:
                result = rail.result(task_id)
                if result:
                    pm_mapping = result
                    break
            except Exception:
                continue

        if isinstance(pm_mapping, list):
            pm_mapping = pm_mapping[0] if pm_mapping else {}
        if isinstance(pm_mapping, dict) and pm_mapping.get('destinationId'):
            pm_pim_id = int(pm_mapping['destinationId'])
            existing = [c for c in resolved if c['id'] == pm_pim_id]
            if existing:
                existing[0]['projectManager'] = True
            else:
                resolved.append({
                    'id': pm_pim_id,
                    'primaryContact': False,
                    'projectManager': True,
                })

        return resolved
    return _inner


def build_lead_contacts_body(class_id):
    """Return a callable that builds EntityContacts.ashx payload from resolved contacts.

    Unlike ``build_entity_contacts_body`` which expects raw mapping data,
    this function works with pre-resolved contacts that already have PIM IDs.
    """
    def _inner():
        pim_id = extract_pim_id_from_mapping()
        if pim_id is None:
            try:
                create_result = rail.result('create_entity')
                if isinstance(create_result, dict):
                    pim_id = create_result.get('id') or create_result.get('Id')
                elif create_result is not None:
                    pim_id = int(create_result)
            except Exception:
                pass
        contacts = rail.result('get_entity_contacts') or []

        body = {
            'id': pim_id,
            'classId': class_id,
            'contact': contacts,
        }
        return json.dumps(body)
    return _inner


# ============================================================================
# 25  Enquiry contacts resolver
# ============================================================================

def resolve_enquiry_contacts(pim_conn_id, d365_conn_id, instance):
    """Return a callable that resolves enquiry contacts/team to PIM IDs.

    Contact list for EntityContacts.ashx UpdateEntityContacts:
    1. Primary contact — ``opportunityid._parentcontactid_value`` on the
       opportunityproduct (ExternalContact), ``primaryContact = true``.
    2. External contacts — from ``vs360_opportunitycontacts``
       (ExternalContact mapping).
    3. Internal team — from ``vs360_opportunityteams``
       (InternalContact mapping), ``projectManager = true`` when the
       employee GUID matches ``_vs360_projectmanager_value``.

    Unmapped external contacts are created inline (testing path).
    Internal team members with no mapping are silently skipped
    (they must be synced via internal_contact_sync first).
    """
    def _inner():
        entity = rail.result('get_d365_entity')
        if not entity:
            return []

        parent_opp = entity.get('opportunityid') or {}
        primary_contact_guid = parent_opp.get('_parentcontactid_value')
        pm_guid = entity.get('_vs360_projectmanager_value')

        d365_token = Variable.get(f'{D365_TOKEN_VAR_PREFIX}_{instance}')
        seen_guids = set()
        resolved = []

        # 1. Primary contact inherited from parent Opportunity
        if primary_contact_guid:
            pim_id = _resolve_external_contact(
                pim_conn_id, d365_conn_id, instance, d365_token, primary_contact_guid,
            )
            if pim_id:
                resolved.append({
                    'id': pim_id,
                    'primaryContact': True,
                    'projectManager': False,
                })
                seen_guids.add(str(primary_contact_guid))

        # 2. External contacts (vs360_opportunitycontacts)
        contacts_raw = rail.result('fetch_enquiry_contacts') or []
        for record in contacts_raw:
            contact_guid = record.get('_vs360_contactid_value')
            if not contact_guid or str(contact_guid) in seen_guids:
                continue
            pim_id = _resolve_external_contact(
                pim_conn_id, d365_conn_id, instance, d365_token, contact_guid,
            )
            if pim_id:
                resolved.append({
                    'id': pim_id,
                    'primaryContact': False,
                    'projectManager': False,
                })
                seen_guids.add(str(contact_guid))

        # 3. Internal team (vs360_opportunityteams) — InternalContact mapping only
        teams_raw = rail.result('fetch_enquiry_team') or []
        for record in teams_raw:
            employee_guid = record.get('_vs360_employeeid_value')
            if not employee_guid or str(employee_guid) in seen_guids:
                continue
            mapping = _get_pim_mapping(
                pim_conn_id, instance,
                MAPPING_TYPE_NAMES['INTERNAL_CONTACT'],
                employee_guid,
            )
            if mapping and mapping.get('destinationId'):
                pim_id = int(mapping['destinationId'])
                resolved.append({
                    'id': pim_id,
                    'primaryContact': False,
                    'projectManager': False,
                })
                seen_guids.add(str(employee_guid))

        pm_mapping = None
        for task_id in ('get_pm_mapping_after_sync', 'get_pm_mapping'):
            try:
                result = rail.result(task_id)
                if result:
                    pm_mapping = result
                    break
            except Exception:
                continue

        if isinstance(pm_mapping, list):
            pm_mapping = pm_mapping[0] if pm_mapping else {}
        if isinstance(pm_mapping, dict) and pm_mapping.get('destinationId'):
            pm_pim_id = int(pm_mapping['destinationId'])
            existing = [c for c in resolved if c['id'] == pm_pim_id]
            if existing:
                existing[0]['projectManager'] = True
            else:
                resolved.append({
                    'id': pm_pim_id,
                    'primaryContact': False,
                    'projectManager': True,
                })

        return resolved
    return _inner

# ============================================================================
# 27  Enquiry team member mapping checker
# ============================================================================

def get_unmapped_team_members(pim_conn_id, instance):
    
    def _inner():
        teams_raw = rail.result('fetch_enquiry_team') or []

        employee_guids = []
        seen = set()
        for record in teams_raw:
            guid = record.get('_vs360_employeeid_value')
            if guid and str(guid) not in seen:
                employee_guids.append(str(guid))
                seen.add(str(guid))

        unmapped = []
        for guid in employee_guids:
            mapping = _get_pim_mapping(
                pim_conn_id, instance,
                MAPPING_TYPE_NAMES['INTERNAL_CONTACT'],
                guid,
            )
            if not mapping or not mapping.get('destinationId'):
                unmapped.append(guid)
        return unmapped
    return _inner


def has_unmapped_team_members():
    """IfOperator test — returns True when there are unmapped team members."""
    unmapped = rail.result('get_unmapped_team_members')
    return bool(unmapped)


def get_unmapped_external_contacts(pim_conn_id, instance, contacts_task_id,
                                   contact_guid_field='_vs360_contactid_value',
                                   primary_contact_getter=None):
    """Return a callable that returns unmapped external contact GUIDs.

    Collects GUIDs from a fetched contacts task (and optional primary contact),
    checks which lack an ExternalContact mapping, and returns the unmapped list.
    Used to trigger external_contact_sync before resolving contacts.
    """
    def _inner():
        guids = []
        seen = set()

        if primary_contact_getter:
            primary = primary_contact_getter()
            if primary:
                guids.append(str(primary))
                seen.add(str(primary))

        contacts_raw = rail.result(contacts_task_id) or []
        for record in contacts_raw:
            guid = record.get(contact_guid_field)
            if guid and str(guid) not in seen:
                guids.append(str(guid))
                seen.add(str(guid))

        unmapped = []
        for guid in guids:
            mapping = _get_pim_mapping(
                pim_conn_id, instance,
                MAPPING_TYPE_NAMES['EXTERNAL_CONTACT'],
                guid,
            )
            if not mapping or not mapping.get('destinationId'):
                unmapped.append(guid)
        return unmapped
    return _inner


def has_unmapped_external_contacts():
    """IfOperator test — returns True when there are unmapped external contacts."""
    unmapped = rail.result('get_unmapped_external_contacts')
    return bool(unmapped)


# ============================================================================
# 28  Enquiry org link body builder
# ============================================================================

def build_link_org_body():
    """Return a callable that builds the organisation link body for an enquiry.

    Reads from fetch_organization_mapping2 first (post-sync path),
    falling back to fetch_organization_mapping (pre-existing mapping path).
    """
    def _inner():
        org_mapping = None
        for task_id in ('fetch_organization_mapping2', 'fetch_organization_mapping'):
            try:
                result = rail.result(task_id)
                if result:
                    org_mapping = result
                    break
            except Exception:
                continue

        if isinstance(org_mapping, list):
            org_mapping = org_mapping[0] if org_mapping else {}
        destination_id = (org_mapping or {}).get('destinationId')
        if not destination_id:
            raise ValueError(
                'Parent organisation mapping not found in PIM — '
                'ensure the External Org sync has run for this account first.'
            )
        return json.dumps({'organization': {'id': int(destination_id)}})
    return _inner


# ============================================================================

def capture_sync_error(entity_type, entity_guid, error_message):
    """Build a structured error dict for sync failures.

    Does **not** raise — the caller decides whether to fail the task.

    Parameters
    ----------
    entity_type : str
        The entity being synced (e.g. ``'Lead'``, ``'Opportunity'``).
    entity_guid : str
        The D365 GUID of the entity that failed.
    error_message : str
        Human-readable error description.

    Returns
    -------
    dict
        ``{'entityType', 'entityGuid', 'error', 'success'}``.
    """
    log.error(
        'Sync error for %s [%s]: %s',
        entity_type,
        entity_guid,
        error_message,
    )
    return {
        'entityType': entity_type,
        'entityGuid': str(entity_guid) if entity_guid else '',
        'error': str(error_message),
        'success': False,
    }

def is_org_mapping_present():
    mapping = rail.result("fetch_organization_mapping")
    if mapping and mapping[0]["destinationId"]:
        return True
    
    else:
        return False
    
# Check is external organization id and role id associated with the project 
# in PIM with the input from the payload
def is_external_organization_associated_with_project(config):
    organizations = rail.result('get_existing_organizations_for_the_project') or []
    target_org_id = rail.result('parse_get_pim_external_organization_id_from_mapper')

    for org in organizations:
        org_id = (org.get('organization') or {}).get('id')
        role_id = (org.get('role') or {}).get('id')

        if org_id == target_org_id and role_id == config.PROJECT_EXTERNAL_ORGANIZATION_ROLE_ID:
            return True

    return False

import uuid
from refinedtechnologies.client_import.utils.custom_function import (
    optional_field,
    optional_clean_field,
    get_country_uri_replicon,
    get_primary_contact,
)
null = None


def _legacy_id_code(salesforce_record):
    raw = salesforce_record.get('Legacy_Id__c')
    if raw in (None, ''):
        return None
    return {"value": int(float(raw))}


# Default billing rates applied when creating a new client (mirrors the recipe).
DEFAULT_BILLING_RATES = {
    "billingRates": [
        {"billingRate": {"uri": null, "name": "Project Rate"}, "rateSchedule": null},
        {"billingRate": {"uri": null, "name": "User Rate"}, "rateSchedule": null},
    ]
}

def search_client_by_code_payload(salesforce_record):
    """Payload to search a Replicon client by Legacy_Id code."""
    legacy_id = salesforce_record.get('Legacy_Id__c', '')
    return {
        "page": 1,
        "pagesize": 10000,
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:client-list-filter:code"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": int(legacy_id)
                }
            }
        },
        "columnUris": [
            "urn:replicon:client-list-column:client",
            "urn:replicon:client-list-column:code",
            "urn:replicon:client-list-column:active"
        ]
    }

def _build_client_modifications(salesforce_record, country_list, contact_result,
                                code_to_apply=None, billing_rates=None):
    """Shared client 'modifications' block; codeToApply/billingRatesToApply are added on create only."""
    contact = get_primary_contact(contact_result)
    contact_email = {"value": contact["email"]} if contact["email"] else {"value": null}
    fax = optional_field("Fax", salesforce_record) or {"value": null}
    phone = optional_field("Phone", salesforce_record)
    website = optional_field("Website", salesforce_record)
    modifications = {
        "nameToApply": optional_field("Name", salesforce_record),
        "descriptionToApply": optional_clean_field("Description", salesforce_record),
        "statusToApply": True,
        "clientContactToApply": {"value": contact["name"]},
        "clientAddressToApply": {
            "address": optional_clean_field("ShippingStreet", salesforce_record),
            "city": optional_field("ShippingCity", salesforce_record),
            "stateProvince": optional_field("ShippingState", salesforce_record),
            "country": get_country_uri_replicon(salesforce_record.get("ShippingCountry"), country_list),
            "zipPostalCode": optional_field("ShippingPostalCode", salesforce_record),
            "phoneNumber": phone,
            "faxNumber": fax,
            "email": contact_email,
            "website": website
        },
        "billingAddressToApply": {
            "address": optional_clean_field("BillingStreet", salesforce_record),
            "city": optional_field("BillingCity", salesforce_record),
            "stateProvince": optional_field("BillingState", salesforce_record),
            "country": get_country_uri_replicon(salesforce_record.get("BillingCountry"), country_list),
            "zipPostalCode": optional_field("BillingPostalCode", salesforce_record),
            "phoneNumber": phone,
            "faxNumber": fax,
            "email": contact_email,
            "website": website
        },
    }
    # Recipe includes these only on the CREATE action (update omits them).
    if code_to_apply is not None:
        modifications["codeToApply"] = code_to_apply
    if billing_rates is not None:
        modifications["billingRatesToApply"] = billing_rates
    return modifications


def update_client_payload(salesforce_record, target_uri, country_list, contact_result=None):
    """Update an existing client (target.uri set, code unchanged, no billing rates)."""
    return {
        "target": {
            "uri": target_uri,
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": _build_client_modifications(
            salesforce_record, country_list, contact_result,
            code_to_apply=null, billing_rates=null
        ),
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def search_user_replicon_payload(username):
    """Payload to search a Replicon user by login name."""
    if not username:
        username = ''
    return {
        'page': '1',
        'pagesize': '100',
        'columnUris': [
            'urn:replicon:user-list-column:login-name'
        ],
        'filterExpression': {
            'leftExpression': {
                'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
            },
            'operatorUri': 'urn:replicon:filter-operator:text-search',
            'rightExpression': {
                'value': {
                    'text': username
                }
            }
        },
    }


def create_new_client_payload(salesforce_record, country_list, contact_result=None):
    """Create a new client (no target, code set from Legacy_Id, default billing rates)."""
    return {
        "target": null,
        "modifications": _build_client_modifications(
            salesforce_record, country_list, contact_result,
            code_to_apply=_legacy_id_code(salesforce_record),
            billing_rates=DEFAULT_BILLING_RATES
        ),
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def update_manager_payload(clientUri, clientManagerUri):
    """Payload to set a Replicon client's manager."""
    return {
        "clientUri": clientUri,
        "clientManagerUri": clientManagerUri
    }

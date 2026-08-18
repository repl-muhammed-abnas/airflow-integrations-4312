import rail
import uuid
from operationalsustainability.sf_client_sync.utils import python_callable

null = None
def search_client_payload(dag_run):
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": [
            "urn:replicon:client-list-column:client",
            "urn:replicon:client-list-column:name"
        ],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:client-list-filter:name"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf.get('item').get('Name'),
                }
            }
        }
    }


def get_search_user_payload():
    user_result = rail.result('search_user_sf')
    records = user_result.get('records', [])

    if not records:
        raise ValueError(f"User not found with OwnerId: {rail.dag_run.conf.get('OwnerId')}")

    username = records[0].get('Username')
    if not username:
        raise ValueError("Username field missing from Salesforce User")

    return {
        "users": [
            {
                "loginName": username
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def does_client_exist():

    search_result = rail.result('search_clients')
    # Client exists if search_result is not None/empty and has a client_uri
    return bool(search_result and search_result.get('client_uri'))


def get_client_manager_login_name():
    user_result = rail.result('search_user')
    if not user_result:
        return None

    if not user_result or not isinstance(user_result, list):
      return None
    d_obj = user_result[0]
    if not d_obj:
        return None

    sec_config = d_obj.get('securityConfiguration')
    if not sec_config:
        return None

    return {
                "uri": null,
                "loginName": sec_config.get('loginName', None),
                "employeeId": null,
                "parameterCorrelationId": null
                }


def get_contact_details():
    contact_result = rail.result('search_contact_sf')
    records = contact_result.get('records', [])

    if not records:
        return {
            'full_name': null,
            'email': null
        }

    contact = records[0]
    first_name = contact.get('FirstName', '')
    last_name = contact.get('LastName', '')

    return {
        'full_name': f"{first_name} {last_name}".strip(),
        'email': contact.get('Email', null)
    }


def apply_client_modifications_payload(dag_run):
    search_result = rail.result('search_clients')
    contact_details = get_contact_details()

    return {
        "target": null if not does_client_exist() else {
            "uri": search_result['client_uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "nameToApply": {
                "value": search_result.get('client_name', dag_run.conf.get('item').get('Name', ''))
            },
            "codeToApply": {
                "value": dag_run.conf.get('item').get('AccountNumber', null)
            },
            "descriptionToApply": {
                "value": dag_run.conf.get('item').get('Description', '')
            },
            "statusToApply": True,
            "clientManagerToApply": {
                "user": get_client_manager_login_name()
                },
            "clientContactToApply": {"value": contact_details['full_name']} if contact_details['full_name'] else null,
            "clientAddressToApply": {
                "address": {"value": dag_run.conf.get('item').get('ShippingStreet', null)},
                "city": {"value": dag_run.conf.get('item').get('ShippingCity', null)},
                "stateProvince": {"value": dag_run.conf.get('item').get('ShippingState', null)},
                "country": {"value": python_callable.convert_to_country_name(dag_run.conf.get('item').get('ShippingCountry'))},
                "zipPostalCode": {"value": dag_run.conf.get('item').get('ShippingPostalCode', null)},
                "phoneNumber": {"value": dag_run.conf.get('item').get('Phone', null)},
                "faxNumber": {"value": dag_run.conf.get('item').get('Fax', null)},
                "email": {"value": contact_details.get('email', null)},
                "website": {"value": dag_run.conf.get('item').get('Website', null)}
            },
            "billingAddressToApply": {
                "address": {"value": dag_run.conf.get('item').get('BillingStreet', null)},
                "city": {"value": dag_run.conf.get('item').get('BillingCity', null)},
                "stateProvince": {"value": dag_run.conf.get('item').get('BillingState', null)},
                "country": {"value": python_callable.convert_to_country_name(dag_run.conf.get('item').get('BillingCountry'))},
                "zipPostalCode": {"value": dag_run.conf.get('item').get('BillingPostalCode', null)}
            }
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


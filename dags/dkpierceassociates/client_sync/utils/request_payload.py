import uuid
import rail
from dkpierceassociates.client_sync import config
from dkpierceassociates.client_sync.utils import custom_function

null = None

def create_client_payload(salesforce):
    country_list = rail.result("get_all_countries")
    if salesforce is not None:
        client_manager_details = {}
        if rail.result("search_replicon_client_manager"):
            client_manager_details = rail.result("search_replicon_client_manager")["records"][0]
        client_details = salesforce.get('records')[0]


        return {
            "target": null,
            "modifications": {
                "nameToApply": {
                "value": client_details["Name"]
                },
                "codeToApply": {
                "value": client_details["Id"]
                },
                "descriptionToApply": {
                "value": client_details["Description"]
                },
                "statusToApply": "true",
                "clientContactToApply": null,
                "clientAddressToApply": {
                "address": {
                    "value": client_details["BillingAddress"]["street"]
                } if client_details["BillingAddress"]["street"] else null,
                "city": {
                    "value": client_details["BillingAddress"]["city"]
                } if client_details["BillingAddress"]["city"] else null,
                "stateProvince": {
                    "value": client_details["BillingAddress"]["state"]
                } if client_details["BillingAddress"]["state"] else null,
                "country": custom_function.get_country_uri_replicon(client_details["BillingAddress"]["country"], country_list) if client_details["BillingAddress"]["country"] else null,
                "zipPostalCode": {
                    "value": client_details["BillingAddress"]["postalCode"]
                } if client_details["BillingAddress"]["postalCode"] else null,
                "phoneNumber": null,
                "faxNumber": null,
                "email": null,
                "website": null
                } if client_details["BillingAddress"] else null,
                "billingAddressToApply": {
                "address": {
                    "value": client_details["BillingAddress"]["street"]
                } if client_details["BillingAddress"]["street"] else null,
                "city": {
                    "value": client_details["BillingAddress"]["city"]
                } if client_details["BillingAddress"]["city"] else null,
                "stateProvince": {
                    "value": client_details["BillingAddress"]["state"]
                } if client_details["BillingAddress"]["state"] else null,
                "country": custom_function.get_country_uri_replicon(client_details["BillingAddress"]["country"], country_list) if client_details["BillingAddress"]["country"] else null,
                "zipPostalCode": {
                    "value": client_details["BillingAddress"]["postalCode"]
                } if client_details["BillingAddress"]["postalCode"] else null,
                "phoneNumber": null,
                "faxNumber": null,
                "email": null,
                "website": null
                } if client_details["BillingAddress"] else null,
                "billingRatesToApply": null,
                "clientManagerToApply": {
                "user": {
                    "uri": client_manager_details["Replicon_Id__c"],
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                }
                } if client_manager_details else null,
                "clientSharingToApply": null,
                "expenseCodesToApply": null,
                "customFieldsToApply": [],
                "taxProfileToApply": null
            },
            "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
            "unitOfWorkId": str(uuid.uuid4())
        }
    
def update_client_payload(salesforce):
    country_list = rail.result("get_all_countries")
    if salesforce is not None:
        client_manager_details = {}
        if rail.result("search_replicon_client_manager"):
            client_manager_details = rail.result("search_replicon_client_manager")["records"][0]
        client_details = salesforce.get('records')[0]

        return {
            "target": {
                "uri": client_details.get("Client_Id_Hiden__c"),
                "name": null,
                "code": null,
                "parameterCorrelationId": null
            }if client_details.get("Client_Id_Hiden__c") else null,
            "modifications": {
                "nameToApply": {
                "value": client_details["Name"]
                },
                "codeToApply": {
                "value": client_details["Id"]
                },
                "descriptionToApply": {
                "value": client_details["Description"]
                },
                "statusToApply": "true",
                "clientContactToApply": null,
                "clientAddressToApply": {
                "address": {
                    "value": client_details["BillingAddress"]["street"]
                } if client_details["BillingAddress"]["street"] else null,
                "city": {
                    "value": client_details["BillingAddress"]["city"]
                } if client_details["BillingAddress"]["city"] else null,
                "stateProvince": {
                    "value": client_details["BillingAddress"]["state"]
                } if client_details["BillingAddress"]["state"] else null,
                "country": custom_function.get_country_uri_replicon(client_details["BillingAddress"]["country"], country_list) if client_details["BillingAddress"]["country"] else null,

                "zipPostalCode": {
                    "value": client_details["BillingAddress"]["postalCode"]
                } if client_details["BillingAddress"]["postalCode"] else null,
                "phoneNumber": null,
                "faxNumber": null,
                "email": null,
                "website": null
                } if client_details["BillingAddress"] else null,
                "billingAddressToApply": {
                "address": {
                    "value": client_details["BillingAddress"]["street"]
                } if client_details["BillingAddress"]["street"] else null,
                "city": {
                    "value": client_details["BillingAddress"]["city"]
                } if client_details["BillingAddress"]["city"] else null,
                "stateProvince": {
                    "value": client_details["BillingAddress"]["state"]
                } if client_details["BillingAddress"]["state"] else null,
                "country": custom_function.get_country_uri_replicon(client_details["BillingAddress"]["country"], country_list) if client_details["BillingAddress"]["country"] else null,
                "zipPostalCode": {
                    "value": client_details["BillingAddress"]["postalCode"]
                } if client_details["BillingAddress"]["postalCode"] else null,
                "phoneNumber": null,
                "faxNumber": null,
                "email": null,
                "website": null
                }if client_details["BillingAddress"] else null,
                "billingRatesToApply": null,
                "clientManagerToApply": {
                "user": {
                    "uri": client_manager_details["Replicon_Id__c"],
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                }
                } if client_manager_details else null,
                "clientSharingToApply": null,
                "expenseCodesToApply": null,
                "customFieldsToApply": [],
                "taxProfileToApply": null
            },
            "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
            "unitOfWorkId": str(uuid.uuid4())
        }


def update_account_salesforce_payload():
    opportunity_data = rail.result("prepare_salesforce_data")['records'][0]
    new_project_data = rail.result("create_client")
    return [{"Id": opportunity_data["Id"],
            "Client_Id_Hiden__c": new_project_data["uri"],
            "Client_URL_Hiden__c": f"{config.replicon_base_url}/clients/details/{new_project_data.get('slug')}",
            }]


def get_client_details_from_replicon_payload(data):
    if data.get("records") :
        client_data = data["records"][0]
        return {
    "clientUri": client_data.get("Client_Id_Hiden__c")
    }

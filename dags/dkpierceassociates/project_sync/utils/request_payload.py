import uuid
import rail
from datetime import datetime
from dkpierceassociates.project_sync.utils import custom_function
from dkpierceassociates.project_sync import config

null = None

def parse_project_manager_login(full_name):
    """
    Convert full name to login format (firstname.lastname).
    Handles edge cases like single names and middle names.

    Args:
        full_name: Full name string (e.g., "John Doe")

    Returns:
        Login name in format "firstname.lastname" or original name if parsing fails
    """
    if not full_name:
        return None

    # Split name and remove empty strings
    name_parts = [part.strip() for part in full_name.split() if part.strip()]

    if len(name_parts) == 0:
        return None
    elif len(name_parts) == 1:
        # Single name: just use it as-is
        return name_parts[0].lower()
    elif len(name_parts) == 2:
        # First and last name: firstname.lastname
        return f"{name_parts[1].lower()}.{name_parts[0].lower()}"
    else:
        # Multiple names: use first and last, ignore middle names
        return f"{name_parts[-1].lower()}.{name_parts[0].lower()}"

def search_client_by_name_payload(data):
    return {
            "page": "1",
            "pagesize": "1000",
            "columnUris": [
                "urn:replicon:client-list-column:active",
                "urn:replicon:client-list-column:code",
                "urn:replicon:client-list-column:client"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:client-list-filter:name"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": data["records"][0]['Name'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            }
            }  

def create_project_payload(salesforce):
    if salesforce is not None:
        account_records = rail.result("search_account_in_salesforce").get('records', [])
        status_value = rail.result("get_all_project_status_labels")
        if not account_records:
            raise ValueError("No account found in Salesforce for the opportunity")

        client_details = account_records[0]
        project_details = salesforce.get('records')[0]

        # Validate required fields
        project_number = project_details.get('Project_Number__c')
        if not project_number:
            raise ValueError(f"Project_Number__c is required but missing for opportunity {project_details.get('Id')}")

        return {
        "target": null,
        "modifications": {
            "nameToApply": {
            "value": project_details.get('Name')
            },
            "codeToApply": {
            "value": str(int(project_number))
            },
            "descriptionToApply": null,
            "percentCompletedToApply": null,
            "startDateToApply": custom_function.get_formatted_date(project_details.get('Start_Date__c')) if project_details.get('Start_Date__c') else null,
            "endDateToApply": custom_function.get_formatted_date(project_details.get('End_Date__c')) if project_details.get('End_Date__c') else null,
            "clientAssignmentsSchedulesToApply": {
            "clients": [
                {
                "client": {
                    "uri": null,
                    "name": client_details.get('Name'),
                    "code": null,
                    "parameterCorrelationId": null
                },
                "costAllocationPercentage": "100"
                }
            ],
            "effectiveDate": null
            },
            "statusToApply": {
            # "uri": "urn:replicon-tenant:186835e34c5a483eb9675c125fd75cb8:project-status-label:67239e89-cd2e-422d-8346-5576776e75e7",
            "uri": custom_function.get_status_uri(status_value),
            "name": null
            },

            "projectLeaderToApply": {
            "user": {
                "uri": null,
                "loginName": parse_project_manager_login(project_details.get('Project_Manager__c')),
                # "loginName": ".".join(project_details.get('Project_Manager__c').lower().split()[::-1]),
                "employeeId": null,
                "parameterCorrelationId": null
            }
            }if project_details.get('Project_Manager__c') else null,

            "estimatedCostToApply": {
            "value": {
                "amount": project_details.get('Amount'),
                "currency": {
                "uri": null,
                "name": null,
                "symbol": "USD$"
                }
            }
            },

            "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
            "billingRateFrequency": null,
            "billingRateFrequencyDuration": null,
            "billingRates": []
            },

        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
        }

def assign_resource_to_project_payload(data):
    resource_uri = rail.result("getAllUserTeamMemberUri")
    return{
        "projectUri": data['uri'],
        "resourceUri": resource_uri,
        "resourceToReplaceUri": null
        }

def get_project_details_payload(project_data):
    return {
        "projectUris": [
            str(project_data.get('Project_Id_Hidden__c'))
        ]
        }


def update_project_payload(salesforce):
    if salesforce is not None:
        account_records = rail.result("search_account_in_salesforce").get('records', [])
        status_value = rail.result("get_all_project_status_labels")

        if not account_records:
            raise ValueError("No account found in Salesforce for the opportunity")

        client_details = account_records[0]
        project_details = salesforce.get('records')[0]

        # Validate required fields
        project_number = project_details.get('Project_Number__c')
        if not project_number:
            raise ValueError(f"Project_Number__c is required but missing for opportunity {project_details.get('Id')}")

        return {
        "target": {
            "uri": project_details.get('Project_Id_Hidden__c'),
        },
        "modifications": {
            "nameToApply": {
            "value": project_details.get('Name')
            },
            "codeToApply": {
            "value": str(int(project_number))
            },
            "descriptionToApply": {
            "value": project_details.get('Description')
            } if project_details.get('Description') else null,
            "percentCompletedToApply": null,
            "startDateToApply": custom_function.get_formatted_date(project_details.get('Start_Date__c')) if project_details.get('Start_Date__c') else null,
            "endDateToApply": custom_function.get_formatted_date(project_details.get('End_Date__c')) if project_details.get('End_Date__c') else null,
            "clientAssignmentsSchedulesToApply": {
            "clients": [
                {
                "client": {
                    "uri": null,
                    "name": client_details.get('Name'),
                    "code": null,
                    "parameterCorrelationId": null
                },
                "costAllocationPercentage": "100"
                }
            ],
            "effectiveDate": null
            },
            "statusToApply": {
            # "uri": config.replicon_project_status_uri,
            "uri": custom_function.get_status_uri(status_value),
            "name": null
            },

            "projectLeaderToApply": {
            "user": {
                "uri": null,
                "loginName": parse_project_manager_login(project_details.get('Project_Manager__c')),
                "employeeId": null,
                "parameterCorrelationId": null
            }
            }if project_details.get('Project_Manager__c') else null,

            "estimatedCostToApply": {
            "value": {
                "amount": project_details.get('Amount'),
                "currency": {
                "uri": null,
                "name": null,
                "symbol": "USD$"
                }
            }
            },

            "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
            "billingRateFrequency": null,
            "billingRateFrequencyDuration": null,
            "billingRates": []
            },

        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
        }

def update_opportunity_salesforce_payload():
    opportunity_data = rail.result("prepare_salesforce_data")['records'][0]
    new_project_data = rail.result("create_project")
    return [{"Id": opportunity_data["Id"],
            "Project_Id_Hidden__c": new_project_data["uri"],
            "Project_URL_Hidden__c": f"{config.replicon_base_url}/projects/details/{new_project_data.get('slug')}",
            }]
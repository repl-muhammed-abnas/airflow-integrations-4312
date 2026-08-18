"""
Request payload builders and response handlers for Salesforce to Polaris project sync
"""
import rail
import uuid
from dateutil.parser import parse as date_parser
from deltek_internal.project_sync import config
from deltek_internal.project_sync.utils import custom_functions
null = None


def parse_salesforce_date_to_replicon_format(date_string):
    """
    Parse Salesforce date string to Replicon date format

    Args:
        date_string: ISO format date string from Salesforce (e.g., "2025-12-09" or "2025-12-09T00:00:00.000Z")

    Returns:
        dict: Date in Replicon format {"year": int, "month": int, "day": int} or None if invalid
    """
    if not date_string:
        return None

    try:
        parsed_date = date_parser(date_string)
        return {
            "year": parsed_date.year,
            "month": parsed_date.month,
            "day": parsed_date.day
        }
    except Exception as e:
        import logging
        logging.warning(f"Failed to parse date '{date_string}': {e}")
        return None


def get_account_name_from_salesforce_query(account_name):
    """
    Get account name from Salesforce by Account ID
    """
    name = account_name[0]['Account']['Name']
    return f"SELECT Id, Name FROM Account WHERE Name = '{name}' LIMIT 1"

def get_case_number_from_salesforce_query(account_name):
    """
    Get case number from Salesforce by Case ID
    """
    case_id = account_name[0]['SOW_CR_Reference__c']
    return f"SELECT CaseNumber from case where Id = '{case_id}' LIMIT 1"

def filter_opportunities_for_processing(dag_run):
    """
    Filter opportunities from Salesforce query result
    Returns list of opportunities ready for processing
    """
    opportunities = rail.result('query_salesforce_opportunities')

    if not opportunities or not isinstance(opportunities, dict):
        return []

    records = opportunities.get('records', [])

    # Filter and prepare opportunities for child DAG processing
    filtered_opportunities = []
    for opp in records:
        # Skip if missing required fields
        if not opp.get('Name') or not opp.get('SOW_CR_Reference__c'):
            continue

        # Determine project type (Enterprise, Change Request, etc.)
        project_type = determine_project_type(opp)

        # Prepare opportunity data for child DAG
        opportunity_data = {
            "opportunity_id": opp.get('Id'),
            "opportunity_name": opp.get('Name'),
            "project_code": opp.get('SOW_CR_Reference__c'),
            "account_id": opp.get('AccountId'),
            "owner_id": opp.get('OwnerId'),
            "probability": opp.get('Probability'),
            "stage_name": opp.get('StageName'),
            "close_date": opp.get('CloseDate'),
            "amount": opp.get('Amount'),
            "project_type": project_type,
            "billing_type": opp.get('Billing_Type__c'),
            "services_package": opp.get('Services_Package__c'),
            "services_add_ons": opp.get('Services_Add_Ons__c'),
            "implementation_start_date": opp.get('Implementation_Start_Date__c'),
            "implementation_end_date": opp.get('Implementation_End_Date__c'),
            "implementation_hours": opp.get('Implementation_Hours__c'),
            "pdm_hours": opp.get('PDM_Hours__c'),
            "cie_hours": opp.get('CIE_Hours__c'),
            "rit_hours": opp.get('RIT_Hours__c'),
            "final_approved_pricing": opp.get('Final_Approved_Pricing__c'),
            "acv": opp.get('ACV__c'),
            "is_closed_won": opp.get('Probability') == 100
        }

        filtered_opportunities.append(opportunity_data)

    return filtered_opportunities


def determine_project_type(opportunity):
    """
    Determine project type based on opportunity data
    Returns: 'enterprise', 'change_request', 'fixed_bid', or 'time_material'
    """
    # Check if it's a change request based on Type or other identifier
    opp_type = opportunity.get('Type', '').lower()

    if 'change' in opp_type or 'cr' in opp_type:
        return 'change_request'

    # Check billing type to determine template
    billing_type = opportunity.get('Billing_Type__c', '').lower()
    if 'fixed' in billing_type:
        return 'fixed_bid'
    elif 't&m' in billing_type or 'time' in billing_type:
        return 'time_material'

    # Default to enterprise implementation
    return 'enterprise'


def build_search_client_payload(client_name):
    """
    Build payload to search for client in Polaris
    """
    return {
            "page": "1",
            "pagesize": "100",
            "columnUris": [
                "urn:replicon:client-list-column:name",
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
                    "text": rail.result("extract_record")[0]["Account"]["Name"],
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
    

def build_create_client_payload(client_name):
    """
    Build payload to create a new client in Polaris
    """
    return {
            "target": null,
            "modifications": {
                "nameToApply": {
                "value": rail.result("extract_record")[0]["Account"]["Name"]
                },
                "codeToApply": null,
                "descriptionToApply": null,
                "statusToApply": True,
                "clientContactToApply": null,
                "clientAddressToApply": null,
                "billingAddressToApply": null,
                "billingRatesToApply": null,
                "clientManagerToApply": null,
                "clientSharingToApply": null,
                "expenseCodesToApply": null,
                "customFieldsToApply": [],
                "taxProfileToApply": null
            },
            "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
            "unitOfWorkId": str(uuid.uuid4())
            }

def build_getProjectWorkflowStateActions():
    project_uri = rail.result("modify_duplicate_project")
    return {
            "project": {
                "uri": project_uri["uri"],
                "name": null,
                "code": null,
                "parameterCorrelationId": null
            }
        }

def build_performProjectWorkflowAction():
    project_uri = rail.result("modify_duplicate_project")
    task_uri = rail.result("getProjectWorkflowStateActions")
    workflow_action_uri = task_uri[0]['actions']
    projectWorkflowActionUri = next((item for item in workflow_action_uri if item.get('displayText') == 'Tentative'), None)
    return {
            "project": {
                "uri": project_uri["uri"],
                "name": null,
                "code": null,
                "parameterCorrelationId": null
            },
            "projectWorkflowActionUri": projectWorkflowActionUri["uri"],
            "unitOfWorkId": str(uuid.uuid4())
            }

def build_update_client_payload():
    project_uri = rail.result("modify_duplicate_project")
    return {
        "projectUri": project_uri["uri"],
        "clients": [
            {
            "client": {
                "uri": rail.result("collect_client_uri"),
                "name": null,
                "code": null,
                "parameterCorrelationId": null
            },
            "costAllocationPercentage": "100"
            }
        ]
        }
    # pass

def build_duplicate_project_payload(extract_record, project_template, case_number):
    """
    Build payload to duplicate a project template
    This creates a new project from a template with "Duplicate Tasks" checkbox selected
    """
    if extract_record and project_template:
        opportunity_data = extract_record[0]
        uri_template = project_template[0].get("customFields")
        project_code = case_number['records'][0].get("CaseNumber")

        return {
            "target": {
                    "uri": null,
                    "name": opportunity_data.get("Name"),
                    "code": null,
                    "parameterCorrelationId": null
                },
            "modifications": {
                "nameToApply": {
                "value": opportunity_data.get("Name")
                },
                "codeToApply": {"value":project_code} if project_code else null,
                "descriptionToApply": null,
                "percentCompletedToApply": null,
                "startDateToApply": null,
                "endDateToApply": null,
                "billingTypeToApply": null,
                "clientBillingAllocationMethodToApply": null,
                "clientAssignmentsSchedulesToApply": null,
                "statusToApply": null,
                "projectWorkflowStateToApply": null,
                "clientRepresentativeToApply": null,
                "programToApply": null,
                "projectLeaderToApply": null,
                "isProjectLeaderApprovalRequired": null,
                "costTypeToApply": null,
                "isTimeEntryAllowed": null,
                "expenseCodesToApply": null,
                "estimatedHoursToApply": null,
                "budgetedHoursToApply": null,
                "estimatedCostToApply": null,
                "budgetedCostToApply": {
                "value": {
                    "amount": "0.0",
                    "currency": {
                    "uri": null,
                    "name": null,
                    "symbol": "USD$"
                    }
                }
                },
                "expenseBudgetedCostToApply": null,
                "totalEstimatedContractValueToApply": {
                    "value": {
                        "amount": str(opportunity_data["Sol_Sales_Total_Services_Pricing__c"]) if opportunity_data["Sol_Sales_Total_Services_Pricing__c"] else null,
                        "currency": {
                        "uri": null,
                        "name": null,
                        "symbol": "USD$"
                        }
                    }
                    },
                "defaultBillingCurrencyToApply": null,
                "timeAndMaterials": null,
                "billingContractToApply": null,
                "fixedBid": null,
                "customFieldsToApply": [
                    {
                        "customField":custom_functions.customFieldsToApply_for_modification_payload(uri_template, filter_value="Opp Name"),
                        "text": str(opportunity_data["Sol_Sales_Opp__c"]) if opportunity_data["Sol_Sales_Opp__c"] else null,
                        "date": null,
                        "dropDownOption": null,
                        "number": null
                    },
                    {   
                        "customField":custom_functions.customFieldsToApply_for_modification_payload(uri_template, filter_value="Current Project Phase"),
                        "text": "Stage 1: Initiation" if opportunity_data['Sol_Sales_Probability__c'] else null,
                        "date": null,
                        "dropDownOption": {
                        'uri': custom_functions.dropdown_uri_for_modification_payload(uri_template, filter_value="Current Project Phase"),
                        "name": null
                        },
                        "number": null
                    },
                    {
                        "customField":custom_functions.customFieldsToApply_for_modification_payload(uri_template, filter_value="Kick-Off Date"),
                        "text": null,
                        "date": parse_salesforce_date_to_replicon_format(opportunity_data.get('Sol_Sales_Proposed_Project_Kick_Off__c')),
                        "dropDownOption": null,
                        "number": null
                    },
                    {
                        "customField":custom_functions.customFieldsToApply_for_modification_payload(uri_template, filter_value="Estimated hours (Impl)"),
                        "text": str(opportunity_data['Sol_Sales_Implementation_Hours__c']) if opportunity_data['Sol_Sales_Implementation_Hours__c'] else null,
                        "date": null,
                        "dropDownOption": null,
                        "number": null
                    },
                    {
                        "customField":custom_functions.customFieldsToApply_for_modification_payload(uri_template, filter_value="SOW Amount (Imp)"),
                        "text": null,
                        "date": null,
                        "dropDownOption": null,
                        "number": str(opportunity_data['Sol_Sales_Implementation_Pricing__c']) if opportunity_data['Sol_Sales_Implementation_Pricing__c'] else null
                    },
                    {
                        "customField":custom_functions.customFieldsToApply_for_modification_payload(uri_template, filter_value="Estimated hours (CIE)"),
                        "text": null,
                        "date": null,
                        "dropDownOption": null,
                        "number": str(opportunity_data['Sol_Sales_CIE_Hours__c']) if opportunity_data['Sol_Sales_CIE_Hours__c'] else null
                    },
                    {
                        "customField":custom_functions.customFieldsToApply_for_modification_payload(uri_template, filter_value="SoW Amount (CIE)"),
                        "text": null,
                        "date": null,
                        "dropDownOption": null,
                        "number": str(opportunity_data['Sol_Sales_CIE_Pricing__c']) if opportunity_data['Sol_Sales_CIE_Pricing__c'] else null
                    },
                    {
                        "customField":custom_functions.customFieldsToApply_for_modification_payload(uri_template, filter_value="Estimated hours (Replicon Integration)"),
                        "text": null,
                        "date": null,
                        "dropDownOption": null,
                        "number": str(opportunity_data['Sol_Sales_RIT_Hours__c']) if opportunity_data['Sol_Sales_RIT_Hours__c'] else null
                    },
                    {
                        "customField":custom_functions.customFieldsToApply_for_modification_payload(uri_template, filter_value="SoW Amount ( RI)"),
                        "text": null,
                        "date": null,
                        "dropDownOption": null,
                        "number": str(opportunity_data['Sol_Sales_RIT_Pricing__c']) if opportunity_data['Sol_Sales_RIT_Pricing__c'] else null
                    }
                    ],
                "resourceAssignmentModifications": null,
                "resourceProjectAssignmentModifications": null,
                "billingContractModifications": null,
                "keyValuesToApply": [],
                "objectExtensionFieldsToApply": [
                    {
                        "definition": {
                        "uri": null,
                        "name": "Estimated hours (PDM)"
                        },
                        "tag": null,
                        "numericValue": str(opportunity_data['Sol_Sales_PDM_Hours__c']) if opportunity_data['Sol_Sales_PDM_Hours__c'] else null,
                        "textValue": null,
                        "fileValue": null,
                        "jsonValue": null
                    }
                    ],
                "portfolioToApply": null,
                "locationToApply": null,
                "divisionToApply": null,
                "serviceCenterToApply": null,
                "costCenterToApply": null,
                "departmentGroupToApply": null,
                "employeeTypeGroupToApply": null
            },
            "projectModificationOptionUri": config.project_modification_save_uri,
            "unitOfWorkId": str(uuid.uuid4())
            }

def build_search_existing_project_payload(response):
    return {
        "projects": [
            {
            "uri": null,
            "name": response[0].get("Name"),
            "code": null,
            "parameterCorrelationId": null
            }
        ]
    }

def build_get_project_template_payload():
    return {
        "projects": [
            {
            "uri": null,
            "name": config.project_template,
            "code": null,
            "parameterCorrelationId": null
            }
        ]
    }
    

def build_create_duplicate_project_payload(response):
    """
    Build payload to create a duplicate project from template

    Args:
        response: List containing Salesforce opportunity record(s)

    Returns:
        dict: Payload for CreateProjectCopyBatch2 service
    """
    opportunity_data = response[0] if response else {}

    # Parse start and end dates from Salesforce opportunity
    start_date = parse_salesforce_date_to_replicon_format(opportunity_data.get('Implementation_Start_Date__c'))
    end_date = parse_salesforce_date_to_replicon_format(opportunity_data.get('Implementation_End_Date__c'))

    return {
        "copyParameter": {
            "sourceProject": {
            "uri": "urn:replicon-tenant:37a97f9d7cd24fda813c761ed12f1770:project:21423",
            "name": null,
            "code": null,
            "parameterCorrelationId": null
            },
            "destinationProjectInfo": {
            "name": opportunity_data.get("Name"),
            "code": null,
            "dateRange": {
                "startDate": start_date,
                "endDate": end_date,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            } if start_date and end_date else null,
            "statusLabel": null,
            "clients": [],
            "program": null,
            "portfolio": null,
            "keyValues": []
            },
            "taskCopyOptionUri": "urn:replicon:project-copy-task-copy-option:copy",
            "teamCopyOptionUri": "urn:replicon:project-copy-team-copy-option:copy",
            "billingRateCopyOptionUri": "urn:replicon:project-copy-billing-rate-copy-option:copy-from-project",
            "expenseCodeCopyOptionUri": "urn:replicon:project-copy-expense-code-copy-option:copy-from-project",
            "taskDateCopyOptionUri": "urn:replicon:task-date-copy-option:copy-date",
            "rateTableEntryCopyOptionUri": "urn:replicon:rate-table-entry-copy-option:copy-from-project",
            "billingContractCopyOptionUri": "urn:replicon:billing-contract-copy-option:copy",
            "projectDependentTimeEntryObjectExtensionFieldCopyOptionUri": "urn:replicon:project-dependent-time-entry-object-extension-field-copy-option:copy",
            "shiftDatesByProjectStartDateOffset": "false",
            "taskResourceEstimatesCopyOptionUri": "urn:replicon:task-resource-estimate-copy-option:copy-estimates-with-resource=selection"
        }
    }

def build_processing_batch_in_background_payload(response):
    return {
        "batchUri": response
                }
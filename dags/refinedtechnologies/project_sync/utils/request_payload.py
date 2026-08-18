import uuid
from datetime import datetime
import rail
from refinedtechnologies.project_sync.utils.custom_function import (
    optional_field,
    optional_clean_field,
    get_country_uri_replicon,
    safe_get_salesforce_record,
    find_matching_client,
    get_primary_contact,
    clean_project_name,
    clean_project_description,
)

# Default billing rates applied when creating a new client (mirrors the recipe).
DEFAULT_BILLING_RATES = {
    "billingRates": [
        {"billingRate": {"uri": None, "name": "Project Rate"}, "rateSchedule": None},
        {"billingRate": {"uri": None, "name": "User Rate"}, "rateSchedule": None},
    ]
}


def default_team_payload():
    """Default team with the tenant's department:1 resource; tenant slug resolved at runtime via rail.get_tenant_slug()."""
    return {
        "teamMembers": [
            {
                "resource": {
                    "uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1",
                    "resourcePlaceholderParameterCorrelationId": None
                }
            }
        ]
    }

def search_project_payload(data):
    record = safe_get_salesforce_record(data)
    if not record or not record.get('RTI_PROJECT_ID__c'):
        return {"projects": []}

    return {
        "projects": [
            {
            "uri":None,
            "name": None,
            "code": record['RTI_PROJECT_ID__c'],
            "parameterCorrelationId": None
            }
        ]
    }

def update_project_branch_payload(opportunity, project_details):
    return {
        "target": {
            "uri": project_details['uri'],
            "name": None,
            "code": None,
            "parameterCorrelationId": None
        },
        "modifications": {
            "nameToApply": {
            "value": clean_project_name(opportunity.get('Replicon_PID_Description__c'))
            },
            "descriptionToApply": {
            "value": clean_project_description(opportunity.get('Description'))
            },

        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
        }

def update_client_payload(project_details, client_reply=None):
    """Apply the sub-child's resolved client to the project (falls back to the existing client if no reply)."""
    client_uri = None
    if client_reply and client_reply.get('clienturi'):
        client_uri = client_reply['clienturi']
    elif project_details.get('client'):
        client_uri = project_details['client'].get('uri')
    return {
        "projectUri": project_details['uri'],
        "clientUri": client_uri,
        "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
    }

def search_user_payload(data):
    record = safe_get_salesforce_record(data)
    username = record.get("Username", "") if record else ""

    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:user-list-column:login-name"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": None,
            "operatorUri": None,
            "rightExpression": None,
            "value": None,
            "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
            "leftExpression": None,
            "operatorUri": None,
            "rightExpression": None,
            "value": {
                "uri": None,
                "uris": [],
                "bool": None,
                "date": None,
                "money": None,
                "number": None,
                "text": username,
                "time": None,
                "calendarDayDurationValue": None,
                "workdayDurationValue": None,
                "dateRange": None,
                "dateTimeUtc": None,
                "dateTimeUtcRange": None,
                "numberRange": None
            },
            "filterDefinitionUri": None
            },
            "value": None,
            "filterDefinitionUri": None
        }
        }

def update_co_manager_payload(project_uri, user_uri):
    if not user_uri or len(user_uri) == 0:
        return {
            "projectUri": project_uri.get('uri') if project_uri else None,
            "sharedUris": []
        }

    return {
        "projectUri": project_uri.get('uri') if project_uri else None,
        "sharedUris": [
            user_uri[0]
            ]
        }

def update_client_manager_payload(client_uri, user_uri):
    manager_uri = user_uri[0] if user_uri and len(user_uri) > 0 else None

    return {
        "clientUri": client_uri.get("uri") if client_uri else None,
        "clientManagerUri": manager_uri
    }

def search_client_by_code_payload(data):
    account_id = data.get('RTI_ACCOUNT_ID__c')
    if account_id is None:
        account_id = ""
    else:
        try:
            account_id = int(account_id)
        except (ValueError, TypeError):
            account_id = ""

    return {
            "page": "1",
            "pagesize": "10000",
            "columnUris": [
                "urn:replicon:client-list-column:client",
                "urn:replicon:client-list-column:code",
                "urn:replicon:client-list-column:active"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": None,
                "filterDefinitionUri": "urn:replicon:client-list-filter:code"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                "leftExpression": None,
                "operatorUri": None,
                "rightExpression": None,
                "value": {
                    "uri": None,
                    "uris": [],
                    "bool": None,
                    "date": None,
                    "money": None,
                    "number": None,
                    "text": account_id,
                    "time": None,
                    "calendarDayDurationValue": None,
                    "workdayDurationValue": None,
                    "dateRange": None,
                    "dateTimeUtc": None,
                    "dateTimeUtcRange": None,
                    "numberRange": None
                },
                "filterDefinitionUri": None
                },
                "value": None,
                "filterDefinitionUri": None
            }
            }

            

def _parse_start_date(start_date):
    """Parse a Salesforce date/datetime string; return a datetime or None."""
    for fmt in ('%Y-%m-%dT%H:%M:%S.%f%z', '%Y-%m-%d'):
        try:
            return datetime.strptime(start_date, fmt)
        except (ValueError, TypeError):
            continue
    return None


def _build_put_project(name, code, description, start_dt, client_uri):
    """Shared PutProject5 body; `code` is pre-formatted by the caller (str vs int)."""
    return {
        "project": {
            "target": {
                "uri": None,
                "name": name,
                "code": None,
                "parameterCorrelationId": None
            },
            "projectInfo": {
                "name": name,
                "code": code,
                "description": description,
                "timeEntryDateRange": {
                    "startDate": {
                        "year": start_dt.year,
                        "month": start_dt.month,
                        "day": start_dt.day
                    },
                    "endDate": None,
                    "relativeDateRangeUri": None,
                    "relativeDateRangeAsOfDate": None
                },
                "projectStatusLabel": {"uri": None, "name": "In Progress"},
                "percentCompleted": "0",
                "clientSchedule": [
                    {
                        "clients": [
                            {
                                "client": {
                                    "uri": client_uri,
                                    "name": None,
                                    "code": None,
                                    "parameterCorrelationId": None
                                },
                                "costAllocationPercentage": "100"
                            }
                        ],
                        "effectiveDate": None
                    }
                ],
                "clientRepresentative": None,
                "program": None,
                "portfolio": None,
                "projectLeader": None,
                "customFieldValues": [],
                "isTimeEntryAllowed": True,
                "costTypeUri": None,
                "clientBillingAllocationMethodUri": None,
                "billingContract": None,
                "revenueContract": None,
                "estimatedHours": None,
                "estimatedCost": None,
                "budgetedHours": None,
                "budgetedCost": None,
                "expenseBudgetedCost": None,
                "resourcebudgetedCost": None,
                "estimatedExpenses": None,
                "budget": None,
                "isProjectLeaderApprovalRequired": False,
                "estimationModeUri": None,
                "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                "timeAndMaterials": {
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                    "billingRateFrequency": None,
                    "billingRateFrequencyDuration": None,
                    "billingRates": []
                },
                "defaultBillingCurrency": None,
                "totalEstimatedContract": None,
                "parentTaskDateRollupOptionUri": None
            },
            "tasks": [],
            "team": default_team_payload(),
            "expenses": None
        }
    }


def create_project_payload(salesforce, replicon_list):
    """Create a project for an already-matched Replicon client."""
    if salesforce is None or replicon_list is None:
        return {}
    project_details = safe_get_salesforce_record(salesforce)
    if not project_details or not project_details.get('Start_Date__c'):
        return {}
    start_dt = _parse_start_date(project_details['Start_Date__c'])
    if start_dt is None or not replicon_list:
        return {}

    matched = find_matching_client(replicon_list, project_details)
    client_uri = matched.get('clienturi') if matched else replicon_list[0].get('clienturi')
    return _build_put_project(
        name=clean_project_name(project_details.get('Replicon_PID_Description__c')),
        code=str(int(project_details.get('RTI_PROJECT_ID__c'))),
        description=clean_project_description(project_details.get('Description')),
        start_dt=start_dt,
        client_uri=client_uri,
    )


def create_project_payload_condition(salesforce, facility_result=None):
    """Create a project using the client resolved by the search-or-create sub-child (or none)."""
    start_dt = _parse_start_date(salesforce.get('Start_Date__c')) or datetime.now()
    # Use the sub-child's resolved clienturi (set for both matched and created clients).
    client_uri = facility_result.get('clienturi') if facility_result else None
    return _build_put_project(
        name=clean_project_name(salesforce.get('Replicon_PID_Description__c')),
        code=str(int(salesforce.get('RTI_PROJECT_ID__c', None))),
        description=clean_project_description(salesforce.get('Description')),
        start_dt=start_dt,
        client_uri=client_uri,
    )

def search_user_create_client_payload(account_detail, contact_detail, country_list):
    salesforce_record = safe_get_salesforce_record(account_detail)
    contact = get_primary_contact(contact_detail)
    contact_email = {"value": contact["email"]} if contact["email"] else {"value": None}

    if not salesforce_record:
        raise ValueError("No valid account record found in Salesforce data")
    shipping_country = salesforce_record.get("ShippingCountry")
    billing_country = salesforce_record.get("BillingCountry")

    return {
        "target": None,
        "modifications": {
            "nameToApply": optional_field("Name", salesforce_record),
            "codeToApply": {"value": int(salesforce_record.get('Legacy_Id__c', None))},
            "descriptionToApply": optional_clean_field("Description", salesforce_record),
            "statusToApply": True,
            "clientContactToApply": {"value": contact["name"]},
            "clientAddressToApply": {
                "address": optional_clean_field("ShippingStreet", salesforce_record),
                "city": optional_field("ShippingCity", salesforce_record),
                "stateProvince": optional_field("ShippingState", salesforce_record),
                "country": get_country_uri_replicon(shipping_country, country_list),
                "zipPostalCode": optional_field("ShippingPostalCode", salesforce_record),
                "phoneNumber": optional_field("Phone", salesforce_record),
                "faxNumber": optional_field("Fax", salesforce_record) or {"value": None},
                "email": contact_email,
                "website": optional_field("Website", salesforce_record)
            },
            "billingAddressToApply": {
                "address": optional_clean_field("BillingStreet", salesforce_record),
                "city": optional_field("BillingCity", salesforce_record),
                "stateProvince": optional_field("BillingState", salesforce_record),
                "country": get_country_uri_replicon(billing_country, country_list),
                "zipPostalCode": optional_field("BillingPostalCode", salesforce_record),
                "phoneNumber": optional_field("Phone", salesforce_record),
                "faxNumber": optional_field("Fax", salesforce_record) or {"value": None},
                "email": contact_email,
                "website": optional_field("Website", salesforce_record)
            },
            "zipPostalCode": optional_field("BillingPostalCode", salesforce_record),
            "phoneNumber": optional_field("Phone", salesforce_record),
            "faxNumber": optional_field("Fax", salesforce_record) or {"value": None},
            "email": contact_email,
            "website": optional_field("Website", salesforce_record),
            "billingRatesToApply": DEFAULT_BILLING_RATES,
            "clientManagerToApply": None,
            "clientSharingToApply": None,
            "expenseCodesToApply": None,
            "customFieldsToApply": [],
            "taxProfileToApply": None
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }
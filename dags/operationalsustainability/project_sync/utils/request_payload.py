from datetime import datetime
from dateutil.relativedelta import relativedelta
from operationalsustainability.project_sync.utils import custom_methods

import rail
import uuid

DATE_FORMAT = "%Y-%m-%d"
null = None

PROJECT_STATUS_URI = (
    "urn:replicon-tenant:32763ebef136406d8187aca3304648cf"
    ":project-status-label:d617a1e5-f071-42cf-ac71-cab057223ba5"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sf_data():
    return rail.result('get_salesforce_trigger_data')


def _custom_field_uri(display_text):
    return rail.find_first_by_attr_and_get_attr(
        rail.result('get_project_custom_fields'),
        'displayText', display_text, 'uri', ''
    )


def _parse_sf_date(date_str):
    if not date_str:
        return null
    dt = datetime.strptime(date_str.split('T')[0].strip(), DATE_FORMAT)
    return {"year": dt.year, "month": dt.month, "day": dt.day}


def _parse_sf_date_plus_months(date_str, months):
    if not date_str:
        return null
    dt = datetime.strptime(date_str.split('T')[0].strip(), DATE_FORMAT) + relativedelta(months=months)
    return {"year": dt.year, "month": dt.month, "day": dt.day}


def _account_record():
    return (rail.result('get_details_of_specific_account').get('records') or [{}])[0]


def _product_name():
    result = rail.result('extract_additional_suffix_of_the_opportunity_product_name')
    return result if result else None


def _client_info():
    return {
        "uri": null,
        "name": (rail.result('search_clients_in_replicon_when_account_id_is_present') or {}).get('client_name')
                or (rail.result('create_client_in_replicon') or {}).get('name'),
        "code": null,
        "parameterCorrelationId": null,
    }


def _build_custom_field_values():
    data = _sf_data()

    field_mapping = [
        ('Amended Amount & Description',        data.get('Amount_of_Amended_Contract__c')),
        ('Amount of Traditional License',        data.get('Amount_of_Traditional_License__c')),
        ('Amount Optional Maintenance Fee %',    data.get('Annual_Optional_Maintenance_Fee__c')),
        ('Annual Escalation Fee %',              data.get('Annual_Escalation_Fee_Percent__c')),
        ('Annual Subscription Amount',           data.get('Annual_Subscription_Amount__c')),
        ('Auto-Renew Unless Notified #Days',     data.get('Auto_Renew_Unless_Notified_Days__c')),
        ('Contract #',                           data.get('Contract__c')),
        ('Date Contract Amended',                data.get('Date_Contract_Amended__c')),
        ('Does Subscription Auto-Renew?',        data.get('Does_Subscription_Auto_Renew__c')),
        ('How Many Subcontractor Hours?',        data.get('How_Many_Subcontractor_Hours__c')),
        ('Is Any International Work Required?',  data.get('Is_Any_International_Work_Required__c')),
        ('License Fee Contract End Date',        data.get('License_Fee_Contract_End_Date__c')),
        ('MSA Terminate Deadline Before AutoRenew', data.get('MSA_Terminate_Deadline_Before_AutoRenew__c')),
        ('OS Project Manager',                   data.get('OS_Project_Manager__c')),
        ('Percent Markup on Expenses',           data.get('Percent_Markup_on_Expenses__c')),
        ('PO#',                                  data.get('PO__c')),
        ('Products (Standard Price Book)',        _product_name()),
        ('SaaS Contract End Date',               data.get('SaaS_Contract_End_Date__c')),
        ('Signed Contract Date',                 data.get('Signed_Subscription_Date__c')),
    ]

    return [
        {"customField": {"uri": _custom_field_uri(name)}, "text": value}
        for name, value in field_mapping
    ]


# ---------------------------------------------------------------------------
# Parent DAG
# ---------------------------------------------------------------------------

def get_new_created_or_updated_opportunity_query():
    look_back = rail.result("get_opportunity_lookback_timestamp")
    current_time = rail.result("get_current_time_in_utc_minus_1_min")

    return f'''SELECT Id,
            Name,
            Type,
            StageName,
            Probability,
            AccountId,
            OwnerId,
            CloseDate,
            Description,
            Amount,
            Billing_Type__c,
            Amount_of_Amended_Contract__c,
            Amount_of_Traditional_License__c,
            Annual_Optional_Maintenance_Fee__c,
            Annual_Escalation_Fee_Percent__c,
            Annual_Subscription_Amount__c,
            Auto_Renew_Unless_Notified_Days__c,
            Contract__c,
            Date_Contract_Amended__c,
            Does_Subscription_Auto_Renew__c,
            How_Many_Subcontractor_Hours__c,
            Is_Any_International_Work_Required__c,
            License_Fee_Contract_End_Date__c,
            MSA_Terminate_Deadline_Before_AutoRenew__c,
            OS_Project_Manager__c,
            Percent_Markup_on_Expenses__c,
            PO__c,
            SaaS_Contract_End_Date__c,
            Signed_Subscription_Date__c
        FROM Opportunity
        WHERE
            LastModifiedDate > {look_back}
            AND LastModifiedDate <= {current_time}
        ORDER BY LastModifiedDate ASC'''


# ---------------------------------------------------------------------------
# Project search / update
# ---------------------------------------------------------------------------

def get_project_details_payload(opportunity_name):
    return {"projects": [{"name": opportunity_name}]}


def update_project_details_payload():
    data = _sf_data()

    return {
        "target": {
            "uri": rail.result('search_projects_in_replicon')[0]['uri'],
        },
        "modifications": {
            "nameToApply": {"value": data['Name']},
            "startDateToApply": {
                "date": _parse_sf_date(data.get('CloseDate')),
            },
            "customFieldsToApply": _build_custom_field_values(),
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4()),
    }


# ---------------------------------------------------------------------------
# Create project
# ---------------------------------------------------------------------------

def _build_create_project_payload(billing_type_uri, include_end_date=False, include_client=True):
    data = _sf_data()
    close_date = data.get('CloseDate')
    is_time_and_material = billing_type_uri == "urn:replicon:billing-type:time-and-material"

    payload = {
        "target": {
            "uri": null,
            "name": data['Name'],
            "code": null,
            "parameterCorrelationId": null,
        },
        "projectInfo": {
            "name": data['Name'],
            "code": null,
            "description": data['Description'][:255] if data.get('Description') else null,
            "timeEntryDateRange": {
                "startDate": _parse_sf_date(close_date),
                "endDate": _parse_sf_date_plus_months(close_date, 1) if include_end_date else null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null,
            },
            "projectStatusLabel": {
                "uri": PROJECT_STATUS_URI,
                "name": null,
            },
            "percentCompleted": "0",
            "clientRepresentative": null,
            "program": null,
            "projectLeader": null,
            "customFieldValues": _build_custom_field_values(),
            "isTimeEntryAllowed": "1",
            "costTypeUri": null,
            "estimatedHours": null,
            "estimatedCost": null,
            "estimatedExpenses": null,
            "budget": null,
            "isProjectLeaderApprovalRequired": "1",
            "estimationModeUri": null,
            "billingTypeUri": billing_type_uri,
            "timeAndMaterials": {
                "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                "billingRateFrequency": null,
                "billingRateFrequencyDuration": null,
                "billingRates": [],
            } if is_time_and_material else null,
            "defaultBillingCurrency": null,
        },
    }

    if include_client:
        payload["projectInfo"]["client"] = _client_info()

    return payload


def create_project_in_replicon_when_billing_type_contains_time_and_material_payload():
    return _build_create_project_payload(
        billing_type_uri="urn:replicon:billing-type:time-and-material",
        include_end_date=False,
        include_client=True,
    )


def create_project_in_replicon_when_billing_type_contains_fixed_bid_payload():
    return _build_create_project_payload(
        billing_type_uri="urn:replicon:billing-type:fixed-bid",
        include_end_date=True,
        include_client=False,
    )


def create_project_in_replicon_when_billing_type_contains_non_billable_payload():
    return _build_create_project_payload(
        billing_type_uri="urn:replicon:billing-type:non-billable",
        include_end_date=False,
        include_client=True,
    )


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

def search_users_in_replicon_payload(login_name=""):
    return {
        "users": [
            {
                "uri": null,
                "loginName": login_name,
                "employeeId": null,
                "parameterCorrelationId": null,
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission",
    }


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

def create_client_in_replicon_payload():
    account = _account_record()
    user_result = rail.result('search_users_in_replicon_when_account_id_is_present') or {}
    user_uri = user_result.get('user_uri')
    contacts = rail.result('search_for_contacts_in_salesforce').get('records', [])

    return {
        "client": {
            "target": {
                "uri": null,
                "name": account.get('Name'),
                "code": null,
                "parameterCorrelationId": null,
            },
            "name": account.get('Name'),
            "code": account.get('AccountNumber'),
            "comment": account.get('Description'),
            "clientManager": {"uri": user_uri} if user_uri else None,
            "billingContact": custom_methods.get_billing_contact_name(contacts),
            "clientAddress": custom_methods.build_client_address(account),
            "billingAddress": custom_methods.build_billing_address(account),
            "isActive": "true",
            "customFieldValues": [],
            "billingRates": [],
            "expenseCodesAllowedByDefaultOnNewProjects": [],
            "defaultBillingCurrency": null,
        }
    }
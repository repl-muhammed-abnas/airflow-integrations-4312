from datetime import datetime
from dateutil.parser import parse as date_parser
from airflow.models import Variable
import rail
import uuid
import json

null = None
true = True

SF_PAYLOAD_DATE_FORMAT = "%Y-%m-%d"

client_address_mapper = {
    "ShippingStreet": "address",
    "ShippingCity": "city",
    "ShippingState": "stateProvince",
    "ShippingPostalCode":  "zipPostalCode",
    "ShippingCountry": "client_country_uri",
    "Phone": "phoneNumber",
    "Fax": "faxNumber",
    "Website": "website"
}

billing_address_mapper = {
    "BillingStreet": "address",
    "BillingCity": "city",
    "BillingState": "stateProvince",
    "BillingPostalCode":  "zipPostalCode",
    "BillingCountry": "billing_country_uri"
}


def get_lookback_period_start_timestamp(config):
    required_lookback_timestamp_accounts = Variable.get(
        config.accounts_lookback_period_start_timestamp)
    required_lookback_timestamp_opportunities = Variable.get(
        config.opportunities_lookback_period_start_timestamp)

    return {
        'accounts_lookback_timestamp': required_lookback_timestamp_accounts,
        'opportunities_lookback_timestamp': required_lookback_timestamp_opportunities
    }


def to_datetime(date, date_format=None):
    if isinstance(date, dict):
        return datetime(day=date['day'], month=date['month'], year=date['year'])
    elif isinstance(date, str):
        return datetime.strptime(date, date_format)
    return date


def convert_to_float_hours(time_dict):
    return float(time_dict.get('hours', 0) + time_dict.get('minutes', 0)/60 + time_dict.get('seconds', 0)/3600) if time_dict else 0.0


def convert_float_to_time_dict(float_hours):
    return {
        "hours": int(float_hours),
        "minutes": int((float_hours % 1) * 60),
        "seconds": int(((float_hours % 1) * 60 % 1) * 60)
    }


def get_new_created_or_updated_account_query(config):
    accounts_lookback_timestamp = rail.result('log_lookback_period_start_timestamp')[
        'accounts_lookback_timestamp']

    # Format the account types properly for SOQL IN clause
    account_types = str(config.ACCOUNT_TYPES_TO_SYNC).replace(
        ',)', ')')  # Remove trailing comma

    company_dba_names_to_exclude_in_sync = str(config.COMPANY_DBA_NAMES_TO_EXCLUDE_IN_SYNC).replace(
        ',)', ')')  # Remove trailing comma

    return f'''SELECT Id,
            Name,
            Type,
            Description,
            OwnerId,
            ia_crm__IntacctID__c,
            ShippingStreet,
            ShippingCity,
            ShippingState,
            ShippingPostalCode,
            ShippingCountry,
            Phone,
            Fax,
            Website,
            BillingStreet,
            BillingCity,
            BillingState,
            BillingPostalCode,
            BillingCountry,
            CreatedDate,
            LastModifiedDate,
            Company_DBA_Name__c
        FROM Account
        WHERE
            Type IN {account_types}
            AND Company_DBA_Name__c NOT IN {company_dba_names_to_exclude_in_sync}
            AND LastModifiedDate > {accounts_lookback_timestamp}
        ORDER BY LastModifiedDate ASC'''


def get_new_created_or_updated_opportunity_query(config):
    excluded_stages_query_condition = []
    for pattern in config.OPPORTUNITY_STAGES_TO_EXCLUDE:
        excluded_stages_query_condition.append(
            f"(NOT StageName LIKE '%{pattern}%')")
    stage_query_condition = ("AND " + " AND ".join(
        excluded_stages_query_condition)) if excluded_stages_query_condition else ""

    opportunities_lookback_timestamp = rail.result(
        'log_lookback_period_start_timestamp')['opportunities_lookback_timestamp']

    return f'''
        SELECT
            Id,
            Name,
            AccountId,
            StageName,
            Amount,
            CloseDate,
            Type,
            Description,
            CreatedDate,
            LastModifiedDate,
            Parent_Opportunity__c,
            Project_Manager__c,
            Project_Start_Date__c,
            Project_End_Date__c,
            Engagement_Type__c,
            Engagement_Manager__c,
            ia_crm__Billing_Type__c,
            Total_Estimated_Hours_Formula__c,
            Total_Contract_Value__c,
            OwnerId,
            CurrencyIsoCode,
            Project_Code__c,
            Engagement_Cost_Center__c
        FROM
            Opportunity
        WHERE
            Parent_Opportunity__c = null
            AND LastModifiedDate > {opportunities_lookback_timestamp}
            {stage_query_condition}
            ORDER BY LastModifiedDate ASC'''


def set_timestamps_based_on_last_modified_record(config):
    sorted_dates_accounts = sorted(list(
        map(lambda record: date_parser(record['LastModifiedDate']), rail.result("new_created_or_updated_account")['records']))) if rail.result(
            "new_created_or_updated_account").get('records', '') else []
    if sorted_dates_accounts:
        Variable.set(
            key=config.accounts_lookback_period_start_timestamp, value=sorted_dates_accounts[-1].strftime(config.TIMESTAMP_DATE_FORMAT))

    sorted_dates_opportunities = sorted(list(
        map(lambda record: date_parser(record['LastModifiedDate']), rail.result("new_created_or_updated_opportunity")['records']))) if rail.result(
            "new_created_or_updated_opportunity").get('records', '') else []
    if sorted_dates_opportunities:
        Variable.set(
            key=config.opportunities_lookback_period_start_timestamp, value=sorted_dates_opportunities[-1].strftime(config.TIMESTAMP_DATE_FORMAT))

    return {
        'updated_accounts_lookback_timestamp': sorted_dates_accounts[-1].strftime(config.TIMESTAMP_DATE_FORMAT) if sorted_dates_accounts else None,
        'updated_opportunities_lookback_timestamp': sorted_dates_opportunities[-1].strftime(config.TIMESTAMP_DATE_FORMAT) if sorted_dates_opportunities else None
    }


def soql_query_for_user_lookup(user_id):
    return f'''
        SELECT
            Id,Username,Name,Email
        FROM
            User
        WHERE
            Id = '{user_id}'
        LIMIT
            1'''


def get_user_email_from_payload(response):
    user_records = response['records']

    return user_records[0].get('Email', '') if user_records else ''


def payload_to_get_all_replicon_clients():
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": [
            "urn:replicon:client-list-column:client",
            "urn:replicon:client-list-column:code",
            "urn:replicon:client-list-column:active"
        ],
        "sort": [],
        "filterExpression": null
    }


def payload_to_get_all_replicon_projects():
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": [
            "urn:replicon:project-list-column:project",
            "urn:replicon:project-list-column:code",
            "urn:replicon:project-list-column:status"
        ],
        "sort": [],
        "filterExpression": null
    }


def get_client_address_fields_add_payload(dag_run):
    client_address_fields_to_add = {}

    for k, v in client_address_mapper.items():
        if dag_run.conf.get(k):
            client_address_fields_to_add.update({
                'country': {
                    "value": {"uri": dag_run.conf[v]}
                }
            } if k == 'ShippingCountry' else {
                v: {
                    "value": dag_run.conf[k]
                }
            })

    return client_address_fields_to_add if client_address_fields_to_add else null


def get_client_address_fields_update_payload(update_logs, dag_run, client_details):
    client_address_fields_to_update = {}

    for k, v in client_address_mapper.items():
        if k == 'ShippingCountry':
            if dag_run.conf.get(k) and (
                    not (client_details['clientAddress']['country']) or client_details['clientAddress']['country'] != dag_run.conf[k]):
                update_logs.append(f"{k} updated")
                client_address_fields_to_update.update({
                    'country': {
                        "value": {"uri": dag_run.conf[v]}
                    }
                })
        else:
            if dag_run.conf.get(k) and (
                    not (client_details['clientAddress'][v]) or client_details['clientAddress'][v] != dag_run.conf[k]):
                update_logs.append(f"{k} updated")
                client_address_fields_to_update.update({
                    v: {
                        "value": dag_run.conf[k]
                    }
                })

    return client_address_fields_to_update if client_address_fields_to_update else null


def get_billing_address_fields_add_payload(dag_run):
    billing_address_fields_to_add = {}

    for k, v in billing_address_mapper.items():
        if dag_run.conf.get(k):
            billing_address_fields_to_add.update({
                'country': {
                    "value": {"uri": dag_run.conf[v]}
                }
            } if k == 'BillingCountry' else {
                v: {
                    "value": dag_run.conf[k]
                }
            })

    return billing_address_fields_to_add if billing_address_fields_to_add else null


def get_billing_address_fields_update_payload(update_logs, dag_run, client_details):
    billing_address_fields_to_update = {}

    for k, v in billing_address_mapper.items():
        if k == 'BillingCountry':
            if dag_run.conf.get(k) and (
                    not (client_details['billingAddress']['country']) or client_details['billingAddress']['country'] != dag_run.conf[k]):
                update_logs.append(f"{k} updated")
                billing_address_fields_to_update.update({
                    'country': {
                        "value": {"uri": dag_run.conf[v]}
                    }
                })
        else:
            if dag_run.conf.get(k) and (
                    not (client_details['billingAddress'][v]) or client_details['billingAddress'][v] != dag_run.conf[k]):
                update_logs.append(f"{k} updated")
                billing_address_fields_to_update.update({
                    v: {
                        "value": dag_run.conf[k]
                    }
                })

    return billing_address_fields_to_update if billing_address_fields_to_update else null


def get_update_client_payload(dag_run):
    update_logs = []
    exceptions = []
    modifications = {}
    client_details = rail.result('get_client_details')

    if dag_run.conf.get('Description') and (
            not (client_details['comment']) or client_details['comment'] != dag_run.conf['Description']):
        update_logs.append(f"Description updated")
        modifications.update({
            "descriptionToApply": {
                "value": dag_run.conf['Description']
            }
        })

    if dag_run.conf.get('OwnerId'):
        current_cm = client_details['clientManager']['user']['loginName'] if client_details['clientManager'] else ''
        if not current_cm or (dag_run.conf.get('OwnerId') != current_cm):
            if rail.result('log_cm_not_found_or_cm_permission_not_found_or_cm_disabled'):
                exceptions.append(rail.result(
                    'log_cm_not_found_or_cm_permission_not_found_or_cm_disabled'))
            else:
                modifications.update({
                    "clientManagerToApply": {
                        "user": {
                            "uri": rail.result('get_client_manager_details')['cm_uri']
                        }
                    }
                })

    modifications.update({
        "clientAddressToApply": get_client_address_fields_update_payload(update_logs, dag_run, client_details),
        "billingAddressToApply": get_billing_address_fields_update_payload(update_logs, dag_run, client_details)
    })
    modifications.update({
        "customFieldsToApply": get_custom_fields(dag_run, exceptions, update_logs, client_details)
    })
    update_client_payload = {
        "target": {
            "uri": dag_run.conf['client_uri'],
        },
        "modifications": modifications,
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

    rail.set_result(key="update_logs", val=update_logs)
    rail.set_result(key="update_exceptions", val=exceptions)

    return update_client_payload


def get_create_client_payload(dag_run):
    exceptions = []

    modifications = {
        "nameToApply": {
            "value": dag_run.conf.get('Name')
        },
        "codeToApply": {
            "value": dag_run.conf.get('Id')
        },
        "descriptionToApply": {
            'value': dag_run.conf.get('Description')
        }if dag_run.conf.get('Description') else null,
        "clientAddressToApply": get_client_address_fields_add_payload(dag_run),
        "billingAddressToApply": get_billing_address_fields_add_payload(dag_run)
    }

    if dag_run.conf.get('OwnerId'):
        if rail.result('log_cm_not_found_or_cm_permission_not_found_or_cm_disabled'):
            exceptions.append(rail.result(
                'log_cm_not_found_or_cm_permission_not_found_or_cm_disabled'))
        else:
            modifications.update({
                "clientManagerToApply": {
                    "user": {
                        "uri": rail.result('get_client_manager_details')['cm_uri']
                    }
                }
            })

    modifications.update({
        "customFieldsToApply": get_custom_fields(dag_run, exceptions)
    })

    rail.set_result(key="create_exceptions", val=exceptions)

    return {
        "target": null,
        "modifications": modifications,
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def validate_mandatory_fields_for_creation(dag_run, config):
    """
    Validate mandatory fields for project creation.
    Returns a list of error messages for missing mandatory fields.
    """

    exception_log = []
    for field in config.MANDATORY_FIELDS_NEW_PROJECT:
        if not dag_run.conf.get(field):
            exception_log.append(f"Mandatory field '{field}' is blank")

    return exception_log


def get_project_copy_batch_param(dag_run):
    return {
        "copyParameter": {
            "sourceProject": {
                "uri": dag_run.conf.get('template_project_uri')
            },
            "destinationProjectInfo": {
                "name": dag_run.conf.get('Name'),
                "code": dag_run.conf.get('Project_Code__c'),
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf.get('Project_Start_Date__c'), SF_PAYLOAD_DATE_FORMAT),
                    "endDate": rail.parse_date(dag_run.conf.get('Project_End_Date__c'), SF_PAYLOAD_DATE_FORMAT) if dag_run.conf.get('Project_End_Date__c') else null
                },
                "statusLabel": null,
                "clients": [],
                "program": null,
                "portfolio": null,
                "keyValues": []
            },
            "taskCopyOptionUri": "urn:replicon:project-copy-task-copy-option:copy",
            "teamCopyOptionUri": "urn:replicon:project-copy-team-copy-option:do-not-copy",
            "billingRateCopyOptionUri": "urn:replicon:project-copy-billing-rate-copy-option:do-not-copy",
            "expenseCodeCopyOptionUri": "urn:replicon:project-copy-expense-code-copy-option:do-not-copy",
            "taskDateCopyOptionUri": "urn:replicon:task-date-copy-option:shift-by-project-start-date-offset",
            "rateTableEntryCopyOptionUri": "urn:replicon:rate-table-entry-copy-option:do-not-copy",
            "billingContractCopyOptionUri": "urn:replicon:billing-contract-copy-option:do-not-copy",
            "projectDependentTimeEntryObjectExtensionFieldCopyOptionUri": "urn:replicon:project-dependent-time-entry-object-extension-field-copy-option:do-not-copy",
            "shiftDatesByProjectStartDateOffset": "true",
            "taskResourceEstimatesCopyOptionUri": "urn:replicon:task-resource-estimate-copy-option:do-not-copy"
        }
    }


def get_create_project_payload(dag_run, config):
    """
    Build payload for creating a new project in Replicon.
    Maps Salesforce Opportunity fields to Replicon Project fields.
    """
    exceptions = []

    payload = {
        "target": {
            "uri": rail.result('get_projectcopy_uri')
        },
        "modifications": {
            "descriptionToApply": {
                "value": dag_run.conf.get('Description', '')
            } if dag_run.conf.get('Description') else null,
            "statusToApply": config.PROJECT_STATUS_MAP.get(
                dag_run.conf.get('StageName')),
            "keyValuesToApply": [
                {
                    "keyUri": "urn:replicon:project-key-value-key:project-management-type",
                    "value": {
                        "uri": "urn:replicon:project-management-type:managed"
                    }
                }
            ],
            "isTimeEntryAllowed": true,
            "isProjectLeaderApprovalRequired": true,
            "objectExtensionFieldsToApply": _get_extension_fields(dag_run, config, exceptions, 'create_project')
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

    if dag_run.conf.get('Total_Estimated_Hours_Formula__c'):
        new_budgeted_hours = float(dag_run.conf.get(
            'Total_Estimated_Hours_Formula__c'))
        payload['modifications']['budgetedHoursToApply'] = {
            "duration": convert_float_to_time_dict(new_budgeted_hours)
        }

    if dag_run.conf.get('Total_Contract_Value__c'):
        new_total_contract_value = float(
            dag_run.conf.get('Total_Contract_Value__c'))
        payload['modifications']['totalEstimatedContractValueToApply'] = {
            "value": {
                "amount": new_total_contract_value,
                "currency": {
                    "uri": dag_run.conf.get('currency_uri'),
                } if dag_run.conf.get('currency_uri') else {
                    "name": "US Dollar",  # default currency
                }
            }
        }

    # To remove any default project manager assignment put by COPY service call
    payload['modifications']['projectLeaderToApply'] = {
        "user": null
    }

    if dag_run.conf.get('Project_Manager__c'):
        if rail.result('log_pm_not_found_or_pm_permission_not_found_or_disabled'):
            exceptions.append(rail.result(
                'log_pm_not_found_or_pm_permission_not_found_or_disabled'))

        else:
            payload['modifications']['projectLeaderToApply'] = {
                "user": {
                    "uri": rail.result('get_project_manager_details')['pm_uri'],
                }
            }
    # Add client assignment if client URI is available (either existing or newly created)
    if dag_run.conf.get('client_uri'):
        payload['modifications']['clientAssignmentsSchedulesToApply'] = {
            "clients": [
                {
                    "client": {"uri": dag_run.conf.get('client_uri')},
                    "costAllocationPercentage": 100
                }
            ],
            "effectiveDate": rail.parse_date(dag_run.conf.get('Project_Start_Date__c'), SF_PAYLOAD_DATE_FORMAT)
        }

    if dag_run.conf.get('AccountId') and not (dag_run.conf.get('client_uri')):
        exceptions.append("Client not found in Replicon")

    if dag_run.conf.get('client_uri') and dag_run.conf.get('OwnerId'):
        if rail.result('check_client_representative_assignment_exception'):
            exceptions.append(rail.result(
                'check_client_representative_assignment_exception'))
        else:
            payload['modifications']['clientRepresentativeToApply'] = {
                "user": {
                    "uri": rail.result('get_client_representative_details_in_replicon').get('uri')
                }
            }

    if dag_run.conf.get('currency_uri'):
        payload['modifications']['defaultBillingCurrencyToApply'] = {
            "currency": {
                "uri": dag_run.conf.get('currency_uri')
            }
        }

    apply_cost_center(payload, dag_run, exceptions)

    if bool(dag_run.conf['Engagement_Manager__c']) and rail.result('log_em_assignment_exceptions'):
        exceptions.append(rail.result('log_em_assignment_exceptions'))

    rail.set_result(key="create_exceptions", val=exceptions)

    return payload


def get_update_project_payload(dag_run, existing_project, config):
    """
    Build payload for updating an existing project in Replicon.
    Only includes fields that have changed from the existing project.
    """
    exceptions = []
    update_logs = []
    is_tcv_updated = False  # Flag to track if Total Contract Value has been updated

    # Target the existing project for update
    payload = {
        "target": {
            "uri": dag_run.conf.get('project_uri')
        },
        "modifications": {},
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

    # Only update fields that have changed
    if dag_run.conf.get('Name') and dag_run.conf.get('Name') != existing_project.get('name'):
        update_logs.append("Name Updated")
        payload['modifications']['nameToApply'] = {
            "value": dag_run.conf.get('Name')
        }

    if dag_run.conf.get('Description') and dag_run.conf.get('Description') != existing_project.get('description'):
        update_logs.append("Description Updated")
        payload['modifications']['descriptionToApply'] = {
            "value": dag_run.conf.get('Description')
        }

    if dag_run.conf.get('currency_uri'):
        payload['modifications']['defaultBillingCurrencyToApply'] = {
            "currency": {
                "uri": dag_run.conf.get('currency_uri')
            }
        }

    # Update dates if changed
    if dag_run.conf.get('Project_Start_Date__c'):
        start_date = to_datetime(dag_run.conf.get(
            'Project_Start_Date__c'), SF_PAYLOAD_DATE_FORMAT)
        existing_project_start_date = (to_datetime(existing_project.get('timeEntryDateRange').get('startDate')) if (
            existing_project.get('timeEntryDateRange').get('startDate')) else None) if existing_project.get('timeEntryDateRange') else None
        if not (existing_project_start_date) or start_date != existing_project_start_date:
            update_logs.append("Start Date Updated")
            payload['modifications']['startDateToApply'] = {
                "date": rail.get_replicon_date(start_date)
            }

    if dag_run.conf.get('Project_End_Date__c'):
        end_date = to_datetime(dag_run.conf.get(
            'Project_End_Date__c'), SF_PAYLOAD_DATE_FORMAT)
        existing_project_end_date = (to_datetime(existing_project.get('timeEntryDateRange').get('endDate')) if (
            existing_project.get('timeEntryDateRange').get('endDate')) else None) if existing_project.get('timeEntryDateRange') else None
        if not (existing_project_end_date) or end_date != existing_project_end_date:
            update_logs.append("End Date Updated")
            payload['modifications']['endDateToApply'] = {
                "date": rail.get_replicon_date(end_date)
            }

    if dag_run.conf.get('Total_Estimated_Hours_Formula__c'):
        new_budgeted_hours = float(dag_run.conf.get(
            'Total_Estimated_Hours_Formula__c'))
        existing_budgeted_hours = convert_to_float_hours(existing_project.get(
            'budgetedHours') if existing_project.get('budgetedHours') else {})
        if (not (existing_project.get('budgetedHours')) or new_budgeted_hours != existing_budgeted_hours):
            update_logs.append("Total Budgeted Hours Updated")
            payload['modifications']['budgetedHoursToApply'] = {
                "duration": convert_float_to_time_dict(new_budgeted_hours)
            }

    if dag_run.conf.get('Total_Contract_Value__c'):
        new_total_contract_value = float(
            dag_run.conf.get('Total_Contract_Value__c'))
        existing_total_contract_value = float(existing_project.get('totalEstimatedContract').get(
            'amount', 0)) if existing_project.get('totalEstimatedContract') else 0
        if not (existing_project.get('totalEstimatedContract')) or new_total_contract_value != existing_total_contract_value:
            update_logs.append("Total Contract Value Updated")
            is_tcv_updated = True
            payload['modifications']['totalEstimatedContractValueToApply'] = {
                "value": {
                    "amount": new_total_contract_value,
                    "currency": {
                        "uri": dag_run.conf.get('currency_uri'),
                    } if dag_run.conf.get('currency_uri') else {
                        "name": "US Dollar",  # default currency
                    }
                }
            }

    if dag_run.conf.get('Project_Manager__c'):
        existing_pm_uri = existing_project.get('projectLeader').get(
            'uri') if existing_project.get('projectLeader') else ''
        if rail.result('log_pm_not_found_or_pm_permission_not_found_or_disabled'):
            exceptions.append(rail.result(
                'log_pm_not_found_or_pm_permission_not_found_or_disabled'))
        else:
            new_pm_uri = rail.result('get_project_manager_details')['pm_uri']
            if new_pm_uri != existing_pm_uri:
                update_logs.append("Project Manager Updated")
                payload['modifications']['projectLeaderToApply'] = {
                    "user": {
                        "uri": new_pm_uri,
                    }
                }

    if dag_run.conf.get('client_uri'):
        existing_client_uri = (existing_project.get('clients')[0].get(
            'client').get('uri')) if existing_project.get('clients') else ''
        if existing_client_uri != dag_run.conf.get('client_uri'):
            update_logs.append("Client Updated")
            payload['modifications']['clientAssignmentsSchedulesToApply'] = {
                "clients": [
                    {
                        "client": {
                            "uri": dag_run.conf.get('client_uri')
                        },
                        "costAllocationPercentage": 100
                    }
                ],
                "effectiveDate": rail.parse_date(dag_run.conf.get('Project_Start_Date__c'), SF_PAYLOAD_DATE_FORMAT)
            }

    if dag_run.conf.get('AccountId') and not (dag_run.conf.get('client_uri')):
        exceptions.append("Client not found in Replicon")

    if dag_run.conf.get('client_uri') and dag_run.conf.get('OwnerId'):
        if rail.result('check_client_representative_assignment_exception'):
            exceptions.append(rail.result(
                'check_client_representative_assignment_exception'))
        else:
            update_logs.append("Client Representative Updated")
            payload['modifications']['clientRepresentativeToApply'] = {
                "user": {
                    "uri": rail.result('get_client_representative_details_in_replicon').get('uri')
                }
            }

    # Update status if changed
    new_status = config.PROJECT_STATUS_MAP.get(
        dag_run.conf.get('StageName'), {'name': 'In Progress'})
    existing_project_status = existing_project.get('status').get(
        'displayText') if existing_project.get('status') else ''
    if new_status and new_status.get('name') != existing_project_status:
        update_logs.append("Project status Updated")
        payload['modifications']['statusToApply'] = new_status

    payload['modifications']['objectExtensionFieldsToApply'] = _get_extension_fields(
        dag_run, config, exceptions, 'update_project', update_logs, existing_project)

    if bool(dag_run.conf['Engagement_Manager__c']):
        if rail.result('log_em_assignment_exceptions'):
            exceptions.append(rail.result('log_em_assignment_exceptions'))
        else:
            update_logs.append("Co-Manager Updated")

    # if dag_run.conf.get('ia_crm__Billing_Type__c'):
    #     payload['modifications']['billingTypeToApply'] = _get_billing_type_mapping(config, dag_run.conf.get('ia_crm__Billing_Type__c'))
    apply_cost_center(payload, dag_run, exceptions, existing_cost_center=existing_project.get(
        "costCenter"), update_logs=update_logs)

    rail.set_result(key="update_logs", val=update_logs)
    rail.set_result(key="update_exceptions", val=exceptions)
    rail.set_result(key="is_tcv_updated", val=is_tcv_updated)

    return payload


def apply_cost_center(payload, dag_run, exceptions, existing_cost_center=None, update_logs=None):
    salesforce_cost_center = dag_run.conf.get('Engagement_Cost_Center__c')
    if not salesforce_cost_center:
        exceptions.append("Cost Center not present in Salesforce")
        return
    matching_cost_center = dag_run.conf.get('matching_cost_center')

    if not matching_cost_center:
        exceptions.append(
            f"Cost Center {salesforce_cost_center} not found in Replicon")
        return

    if not matching_cost_center.get("cost_center_enabled"):
        exceptions.append(
            f"Cost Center {salesforce_cost_center} not enabled in Replicon"
        )
        return
    if existing_cost_center and existing_cost_center.get("uri") == matching_cost_center.get("cost_center_uri"):
        return
    payload["modifications"]["costCenterToApply"] = {
        "costCenter": {
            "uri": matching_cost_center.get("cost_center_uri")
        }
    }

    if update_logs is not None:
        update_logs.append("Cost Center updated")


def _get_extension_fields(dag_run, config, exceptions, process_type, update_logs=[], existing_project=None):
    """
    Build object extension fields array for project categorization.
    Maps engagement types to Productive or Prod Support flags.
    """
    extension_fields = []

    id = dag_run.conf.get('Id')

    # Map Engagement Type to Prod Support/Productive OEF field
    engagement_type = dag_run.conf.get('Engagement_Type__c')
    if engagement_type:
        if engagement_type in config.PRODUCTIVE_ENGAGEMENT_TYPES:
            update_logs.append("Prod Support/Productive OEF value updated")
            extension_fields.append({
                "definition": {
                    "name": "Prod Support/Productive"
                },
                "tag": {
                    "tagName": {
                        "name": "Productive",
                        "tagDefinitionUri": null
                    }
                }
            })
        elif engagement_type in config.PROD_SUPPORT_ENGAGEMENT_TYPES:
            update_logs.append("Prod Support/Productive OEF value updated")
            extension_fields.append({
                "definition": {
                    "name": "Prod Support/Productive"
                },
                "tag": {
                    "tagName": {
                        "name": "Prod Support",
                        "tagDefinitionUri": null
                    }
                }
            })
        else:
            exceptions.append(
                "Prod Support/Productive OEF not updated as Engagement Type is Invalid")

    # Engagement Stage OEF update
    apply_engagement_stage_oef(
        dag_run, extension_fields, exceptions, update_logs, existing_project)

    # Engagement type OEF is not supposed to be Updated
    if process_type == 'create_project':
        if engagement_type:
            if dag_run.conf.get('engagement_type_oef_uri'):
                if dag_run.conf.get('matching_engagement_type_from_oef_dd'):
                    update_logs.append("Engagement Type OEF value updated")
                    extension_fields.append({
                        "definition": {
                            "uri": dag_run.conf.get('engagement_type_oef_uri')
                        },
                        "tag": {
                            "tagName": {
                                "name": engagement_type,
                                "tagDefinitionUri": null
                            }
                        }
                    })
                else:
                    exceptions.append(
                        "Engagement Type not found in Dropdown values for Engagement Type OEF")
            else:
                exceptions.append(
                    "Engagement Type OEF not found")

    # Salesforce ID OEF update
    if id:
        update_logs.append("Salesforce ID OEF value updated")
        extension_fields.append({
            "definition": {
                "name": "Salesforce ID"
            },
            "textValue": id,
        })

    return extension_fields


def cost_center_list_service_get_data_payload():
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:effectively-enabled",
        ],
        "sort": [],
        "filterExpression": null
    }


def apply_engagement_stage_oef(dag_run, extension_fields, exceptions, update_logs, existing_project=None):
    engagement_stage = dag_run.conf.get('StageName')
    if not engagement_stage:
        exceptions.append("Engagement Stage not found in Salesforce")
        return
    if not dag_run.conf.get('engagement_stage_oef_uri'):
        exceptions.append("Engagement Stage OEF not found")
        return
    if not dag_run.conf.get('matching_engagement_stage_from_oef_dd'):
        exceptions.append(
            "Engagement Stage not found in Dropdown values for Engagement Stage OEF")
        return
    if existing_project:
        extension_field_values = existing_project.get("extensionFieldValues")
        existing_engagement_stage = rail.find_first_by_attr_and_get_attr(
            extension_field_values, "definition.uri", dag_run.conf.get(
                'engagement_stage_oef_uri'), "tag.displayText"
        )
        if existing_engagement_stage == engagement_stage:
            return
    extension_fields.append({
        "definition": {
            "uri": dag_run.conf.get('engagement_stage_oef_uri')
        },
        "tag": {
            "tagName": {
                "name": engagement_stage,
                "tagDefinitionUri": null
            }
        }
    })
    update_logs.append("Engagement Stage OEF value updated")


def get_custom_fields(dag_run, exceptions, update_logs=None, client_details=None):
    custom_fields = []
    # Apply Intacct ID UDF
    apply_intacct_id_udf(custom_fields, dag_run, exceptions,
                         update_logs, client_details)

    return custom_fields


def apply_intacct_id_udf(custom_fields, dag_run, exceptions, update_logs=None, client_details=None):
    """
    Apply / update Intacct ID UDF from Salesforce into Replicon custom fields.
    """

    salesforce_intacct_id = dag_run.conf.get("ia_crm__IntacctID__c")
    intacct_id_udf = dag_run.conf.get("intacct_id_udf")

    # Validate Salesforce Intacct ID
    if not salesforce_intacct_id:
        exceptions.append("Intacct ID not present in Salesforce")
        return custom_fields

    # Validate Replicon UDF URI
    if not intacct_id_udf:
        exceptions.append("Intacct ID UDF not found in Replicon")
        return custom_fields

    # Validate maximum length of Intacct ID
    if intacct_id_udf.get("textConfiguration") and intacct_id_udf.get("textConfiguration").get("maximumLength"):
        max_length = intacct_id_udf.get(
            "textConfiguration").get("maximumLength")
        if len(salesforce_intacct_id) > max_length:
            exceptions.append(
                f"Intacct ID length exceeds maximum allowed length of {max_length} characters in Replicon")
            return custom_fields

    intacct_id_udf_uri = intacct_id_udf.get("uri")
    # Check if update is required when client already exists
    if client_details:
        existing_intacct_id = rail.find_first_by_attr_and_get_attr(
            client_details.get(
                "customFields"), "customField.uri", intacct_id_udf_uri, "text",
        )

        if existing_intacct_id == salesforce_intacct_id:
            return custom_fields

    # Append / update Intacct ID UDF
    custom_fields.append({
        "customField": {
            "uri": intacct_id_udf_uri,
            "name": null,
            "groupUri": null,
        },
        "text": salesforce_intacct_id,
        "date": null,
        "dropDownOption": null,
        "number": null,
    })

    if update_logs is not None:
        update_logs.append("Intacct ID UDF updated")

    return custom_fields


from datetime import datetime
import uuid
import json
import rail

null = None

TEXT_CUSTOM_FIELDS = {
    'Capex Number': 'capex_number',
    'Opportunity ID': 'opportunity_id',
    'Client Name': 'client_name',
    'Requested By': 'requested_by'
}

DROPDOWN_CUSTOM_FIELDS = {
    'Project Type': 'project_type',
    'CapSW Sub category': 'capsw_sub_category',
    'R&D': 'rnd',
    'Project Category': 'project_category',
}


def get_project_copy_batch_param(dag_run):
    return {
        "copyParameter": {
            "sourceProject": {
                "uri": dag_run.conf["template_project_uri"]},
            "destinationProjectInfo": {
                "name": dag_run.conf["project_name"],
                "code": dag_run.conf["project_code"],
                "dateRange": {
                    "startDate": rail.get_replicon_date(datetime.strptime(dag_run.conf["start_date"], '%m/%d/%Y')),
                    "endDate": rail.get_replicon_date(datetime.strptime(dag_run.conf["end_date"], '%m/%d/%Y')) if dag_run.conf["end_date"] else null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "statusLabel": null,
                "clients": []},
            "taskCopyOptionUri": "urn:replicon:project-copy-task-copy-option:copy",
            "teamCopyOptionUri": "urn:replicon:project-copy-team-copy-option:copy",
            "billingRateCopyOptionUri": "urn:replicon:project-copy-billing-rate-copy-option:copy-from-project",
            "expenseCodeCopyOptionUri": "urn:replicon:project-copy-expense-code-copy-option:copy-from-project",
            "taskDateCopyOptionUri": "urn:replicon:task-date-copy-option:copy-date",
            "rateTableEntryCopyOptionUri": "urn:replicon:rate-table-entry-copy-option:copy-from-project",
            "billingContractCopyOptionUri": "urn:replicon:billing-contract-copy-option:copy",
            "projectDependentTimeEntryObjectExtensionFieldCopyOptionUri": "urn:replicon:project-dependent-time-entry-object-extension-field-copy-option:copy",
            "shiftDatesByProjectStartDateOffset": "false"}}


def create_projectorapply_modification_param(dag_run):
    payload = {
        "target": {
            "uri": null,
            "name": null,
            "code": dag_run.conf["project_code"],
            "parameterCorrelationId": null
        },
        "modifications": {
            "nameToApply": null,
            "codeToApply": null,
            "descriptionToApply": {"value": dag_run.conf["project_description"]},
            "percentCompletedToApply": null,
            "startDateToApply": null,
            "endDateToApply": null,
            "billingTypeToApply": null,
            "clientBillingAllocationMethodToApply": null,
            "clientAssignmentsSchedulesToApply": null,
            "statusToApply": {
                "name": dag_run.conf["project_status"]
            },
            "projectWorkflowStateToApply": null,
            "clientRepresentativeToApply": null,
            "programToApply": null,
            "projectLeaderToApply": {
                "user": {
                    "uri": rail.result("get_project_manager_details"),
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                }
            },
            "isProjectLeaderApprovalRequired": "true" if dag_run.conf['attestation'].lower() == 'yes' else "false",
            "costTypeToApply": null,
            "isTimeEntryAllowed": "false",
            "estimatedHoursToApply": null,
            "budgetedHoursToApply": null,
            "estimatedCostToApply": null,
            "budgetedCostToApply": null,
            "expenseBudgetedCostToApply": null,
            "totalEstimatedContractValueToApply": null,
            "defaultBillingCurrencyToApply": null,
            "timeAndMaterials": {
                "timeAndExpenseEntryTypeUri": get_time_and_exp_entry_uri(dag_run.conf["billable_non_billable"]),
                "billingRateFrequency": null,
                "billingRateFrequencyDuration": null,
                "billingRates": []
            },
            "billingContractToApply": null,
            "fixedBid": null,
            "customFieldsToApply": get_custom_fields_to_add(dag_run),
            "resourceAssignmentModifications": null,
            "resourceProjectAssignmentModifications": null,
            "billingContractModifications": null,
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": [],
            "portfolioToApply": null,
            "locationToApply": null,
            "divisionToApply": null,
            "serviceCenterToApply": null,
            "costCenterToApply": {
                "costCenter": {
                    "uri": dag_run.conf['cost_center_uri'],
                    "parentUri": null,
                    "name": null
                }
            },
            "departmentGroupToApply": null,
            "employeeTypeGroupToApply": null
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

    return payload


def get_time_and_exp_entry_uri(value):
    if value == 'Bill':
        return 'urn:replicon:time-and-expense-entry-type:billable'
    elif value == 'Non Bill':
        return 'urn:replicon:time-and-expense-entry-type:non-billable'
    else:
        return None


def get_custom_fields_to_add(dag_run):
    payload = []
    for i, v in TEXT_CUSTOM_FIELDS.items():
        if dag_run.conf[v]:
            payload.append(
                {
                    "customField": {
                        "uri": rail.find_first_by_attr_and_get_attr(dag_run.conf['get_all_custom_fields'], 'displayText', i, 'uri'),
                        "name": null,
                        "groupUri": null
                    },
                    "text": dag_run.conf[v],
                    "date": null,
                    "dropDownOption": null,
                    "number": null
                }
            )

    for i, v in DROPDOWN_CUSTOM_FIELDS.items():
        if dag_run.conf[v]:
            payload.append(
                {
                    "customField": {
                        "uri": rail.find_first_by_attr_and_get_attr(dag_run.conf['get_all_custom_fields'], 'displayText', i, 'uri'),
                        "name": null,
                        "groupUri": null
                    },
                    "text": null,
                    "date": null,
                    "dropDownOption": {
                        "uri": null,
                        "name": dag_run.conf[v]
                    },
                    "number": null
                })
    return payload


def update_projectorapply_modification_param(dag_run):
    payload = {
        "target": {
            "uri": null,
            "name": null,
            "code": dag_run.conf["project_code"],
            "parameterCorrelationId": null
        },
        "modifications": {
            "nameToApply": get_updated_project_name(dag_run),
            "codeToApply": null,
            "descriptionToApply": get_updated_project_description(dag_run),
            "percentCompletedToApply": null,
            "startDateToApply": get_updated_start_date(dag_run),
            "endDateToApply": get_updated_end_date(dag_run),
            "billingTypeToApply": null,
            "clientBillingAllocationMethodToApply": null,
            "clientAssignmentsSchedulesToApply": null,
            "statusToApply": get_updated_status_to_apply(dag_run),
            "projectWorkflowStateToApply": null,
            "clientRepresentativeToApply": null,
            "programToApply": null,
            "projectLeaderToApply": get_updated_project_leader(dag_run),
            "isProjectLeaderApprovalRequired": get_project_manager_approval(dag_run),
            "costTypeToApply": null,
            "isTimeEntryAllowed": null,
            "estimatedHoursToApply": null,
            "budgetedHoursToApply": null,
            "estimatedCostToApply": null,
            "budgetedCostToApply": null,
            "expenseBudgetedCostToApply": null,
            "totalEstimatedContractValueToApply": null,
            "defaultBillingCurrencyToApply": null,
            "timeAndMaterials": get_updated_time_and_exp_entry(dag_run),
            "billingContractToApply": null,
            "fixedBid": null,
            "customFieldsToApply": get_custom_fields_to_update(dag_run),
            "resourceAssignmentModifications": null,
            "resourceProjectAssignmentModifications": null,
            "billingContractModifications": null,
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": [],
            "portfolioToApply": null,
            "locationToApply": null,
            "divisionToApply": null,
            "serviceCenterToApply": null,
            "costCenterToApply": get_updated_cost_center(dag_run),
            "departmentGroupToApply": null,
            "employeeTypeGroupToApply": null
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

    return payload


def get_updated_project_name(dag_run):
    if dag_run.conf["project_name"] and dag_run.conf["project_name"] != rail.result('get_project_details')["name"]:
        return {"value": dag_run.conf["project_name"]}
    return null

def get_updated_project_description(dag_run):
    if dag_run.conf["project_description"] and dag_run.conf["project_description"] != rail.result('get_project_details')["description"]:
        return {"value": dag_run.conf["project_description"]}
    return null


def get_updated_status_to_apply(dag_run):
    if dag_run.conf["project_status"] and dag_run.conf["project_status"] != rail.result('get_project_details')['status']['displayText']:
        return {
            "name": dag_run.conf["project_status"]
        }
    return null


def get_updated_project_leader(dag_run):
    if dag_run.conf["project_manager_id"] and rail.result('get_project_details')['projectLeader'] and \
            rail.result('get_project_details')['projectLeader']['uri'] != rail.result('get_project_manager_details'):
        return {
            "user": {
                "uri": rail.result("get_project_manager_details"),
                "loginName": null,
                "employeeId": null,
                "parameterCorrelationId": null
            }
        }
    return null


def get_project_manager_approval(dag_run):
    if rail.result('get_project_details')['isProjectLeaderApprovalRequired'] != dag_run.conf['attestation'].lower():
        return "true" if dag_run.conf['attestation'].lower() == 'yes' else "false"
    return null


def get_updated_time_and_exp_entry(dag_run):
    if dag_run.conf["billable_non_billable"] in ['Bill', 'Non Bill'] and rail.result('get_project_details')['timeAndExpenseEntryType'] and \
            get_time_and_exp_entry_uri(dag_run.conf["billable_non_billable"]) != rail.result('get_project_details')['timeAndExpenseEntryType']['uri']:
        return {
            "timeAndExpenseEntryTypeUri": get_time_and_exp_entry_uri(dag_run.conf["billable_non_billable"]),
            "billingRateFrequency": null,
            "billingRateFrequencyDuration": null,
            "billingRates": []
        }
    return null


def get_custom_fields_to_update(dag_run):
    payload = []
    for i, v in TEXT_CUSTOM_FIELDS.items():
        if dag_run.conf[v] and rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customFields'],
                                                                    'customField.displayText', i, 'text') != dag_run.conf[v]:
            payload.append(
                {
                    "customField": {
                        "uri": rail.find_first_by_attr_and_get_attr(dag_run.conf['get_all_custom_fields'], 'displayText', i, 'uri'),
                        "name": null,
                        "groupUri": null
                    },
                    "text": dag_run.conf[v],
                    "date": null,
                    "dropDownOption": null,
                    "number": null
                }
            )

    for i, v in DROPDOWN_CUSTOM_FIELDS.items():
        if dag_run.conf[v] and rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customFields'], 'customField.displayText', i, 'text') != dag_run.conf[v]:
            payload.append(
                {
                    "customField": {
                        "uri": rail.find_first_by_attr_and_get_attr(dag_run.conf['get_all_custom_fields'], 'displayText', i, 'uri'),
                        "name": null,
                        "groupUri": null
                    },
                    "text": null,
                    "date": null,
                    "dropDownOption": {
                        "uri": null,
                        "name": dag_run.conf[v]
                    },
                    "number": null
                })

    return payload


def get_updated_cost_center(dag_run):
    if rail.result('get_project_details')['costCenter'] and dag_run.conf['cost_center_uri'] != rail.result('get_project_details')['costCenter']['uri']:
        return {
            "costCenter": {
                "uri": dag_run.conf['cost_center_uri'],
                "parentUri": null,
                "name": null
            }
        }
    return null


def get_updated_end_date(dag_run):
    if dag_run.conf["end_date"] and rail.result('get_project_details')['timeEntryDateRange']['endDate'] \
        and (str(rail.result('get_project_details')['timeEntryDateRange']['endDate']['month'])+'/' +
                                     str(rail.result('get_project_details')['timeEntryDateRange']['endDate']['day']) + '/' +
                                     str(rail.result('get_project_details')['timeEntryDateRange']['endDate']['year'])) != dag_run.conf["end_date"]:
        return {
            "date": rail.get_replicon_date(datetime.strptime(dag_run.conf["end_date"], '%m/%d/%Y'))
        }
    if dag_run.conf["end_date"] and not rail.result('get_project_details')['timeEntryDateRange']['endDate']:
        return {
            "date": rail.get_replicon_date(datetime.strptime(dag_run.conf["end_date"], '%m/%d/%Y'))
        }
    return null

def get_updated_start_date(dag_run):
    if dag_run.conf["start_date"] and rail.result('get_project_details')['timeEntryDateRange']['startDate'] \
        and (str(rail.result('get_project_details')['timeEntryDateRange']['startDate']['month'])+'/' +
                                     str(rail.result('get_project_details')['timeEntryDateRange']['startDate']['day']) + '/' +
                                     str(rail.result('get_project_details')['timeEntryDateRange']['startDate']['year'])) != dag_run.conf["start_date"]:
        return {
            "date": rail.get_replicon_date(datetime.strptime(dag_run.conf["start_date"], '%m/%d/%Y'))
        }
    if dag_run.conf["start_date"] and not rail.result('get_project_details')['timeEntryDateRange']['startDate']:
        return {
            "date": rail.get_replicon_date(datetime.strptime(dag_run.conf["start_date"], '%m/%d/%Y'))
        }
    return null


def get_eligibleprojectteammember_dataaccessscopes(dag_run):
    payload = {
        "projectUri": rail.result('create_projectorapply_modifications')['uri'],
        "teamMemberDataAccessScopes": [
            {
                "locations": [],
                "divisions": [],
                "costCenters": [
                    {
                        "uri": dag_run.conf['cost_center_uri'],
                        "parentUri": null,
                        "name": null
                    }
                ],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": []
            }
        ]
    }
    return payload


def get_supervisor_details(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [],
                        "bool": "true",
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
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
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
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
                        "text": dag_run.conf["project_manager_id"],
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
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def get_project_manager_blob_data():
    return {
        "project_uri": rail.result('create_projectorapply_modifications')['uri'],
        "project_manager_uri": rail.result("get_project_manager_details"),
        "effective_date": datetime.now().strftime("%m-%d-%Y")
    }

def add_project_data_to_blob_param(key_namespace):
    return {
        "keyNamespace": key_namespace,
        "keyValue": {
            "key": get_project_manager_blob_data()["project_uri"],
            "jsonValue": json.dumps([get_project_manager_blob_data()])
        }
    }

def update_project_data_to_blob_param(key_namespace):
    existing_data = json.loads(rail.result("get_project_manager_blob_data")['jsonValue'])
    existing_data.append(get_project_manager_blob_data())
    
    return {
        "keyNamespace": key_namespace,
        "keyValue": {
            "key": get_project_manager_blob_data()["project_uri"],
            "jsonValue": json.dumps(existing_data)
        }
    }

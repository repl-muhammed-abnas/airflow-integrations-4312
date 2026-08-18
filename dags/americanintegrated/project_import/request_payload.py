from uuid import uuid4
import rail
null = None
project_status_mapper = {
    "in progress": "In Progress",
    "archived": "Archived",
    "cancelled": "Cancelled",
    "close": "Completed",
    "deferred": "Deferred",
    "tentative": "Tentative",
}


def client_data_request(dag_run):
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:client-list-column:client",
            "urn:replicon:client-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:client-list-filter:code"
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
                    "text": dag_run.conf["clientcode"],
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


def get_client_by_name_request(dag_run):
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
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
                    "text": dag_run.conf["clientname"],
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


def create_client_request(dag_run):
    return {
        "target": null,
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf["clientname"]
            },
            "codeToApply": {
                "value": dag_run.conf["clientcode"]
            },
            "descriptionToApply": null,
            "statusToApply": "true",
            "clientContactToApply": null,
            "clientAddressToApply": null,
            "billingAddressToApply": null,
            "billingRatesToApply": {
                "billingRates": [
                    {
                        "billingRate": {
                            "uri": null,
                            "name": "Project Rate"
                        },
                        "rateSchedule": null
                    },
                    {
                        "billingRate": {
                            "uri": null,
                            "name": "User Rate"
                        },
                        "rateSchedule": null
                    }
                ]
            },
            "clientManagerToApply": null,
            "clientSharingToApply": null,
            "expenseCodesToApply": null,
            "customFieldsToApply": [],
            "taxProfileToApply": null
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }


def get_project_by_code(dag_run):
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            "urn:replicon:project-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:project-list-filter:code"
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
                    "text": dag_run.conf["projectcode"],
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


def create_project_request(dag_run):
    return {
        "target": null,
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf["projectcode"] + " - " + dag_run.conf["projectname"]
            },
            "codeToApply": {
                "value": dag_run.conf["projectcode"]
            },
            "descriptionToApply": null,
            "percentCompletedToApply": "0",
            "startDateToApply": null,
            "endDateToApply": null,
            "billingTypeToApply": {
                "value": "urn:replicon:billing-type:time-and-material"
            },
            "clientBillingAllocationMethodToApply": null,
            "clientAssignmentsSchedulesToApply": {
                "clients": [
                    {
                        "client": {
                            "uri": dag_run.conf['client_uri'],
                            "name": null,
                            "code": null,
                            "parameterCorrelationId": null
                        },
                        "costAllocationPercentage": "0"
                    }
                ],
                "effectiveDate": null
            } if dag_run.conf['client_uri'] else None,
            "statusToApply": {
                "name": project_status_mapper[str(dag_run.conf["projectstatus"]).lower()]
            } if dag_run.conf["projectstatus"] else None,
            "projectWorkflowStateToApply": null,
            "clientRepresentativeToApply": null,
            "programToApply": null,
            "projectLeaderToApply": null,
            "isProjectLeaderApprovalRequired": null,
            "costTypeToApply": null,
            "isTimeEntryAllowed": null,
            "estimatedHoursToApply": null,
            "budgetedHoursToApply": null,
            "estimatedCostToApply": null,
            "budgetedCostToApply": null,
            "expenseBudgetedCostToApply": null,
            "totalEstimatedContractValueToApply": null,
            "defaultBillingCurrencyToApply": null,
            "timeAndMaterials": {
                "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                "billingRateFrequency": null,
                "billingRateFrequencyDuration": null,
                "billingRates": []
            },
            "billingContractToApply": null,
            "fixedBid": null,
            "customFieldsToApply": [
                {
                    "customField": {
                        "uri": dag_run.conf["prevailingwageuri"],
                        "name": null,
                        "groupUri": "urn:replicon:object-type:project"
                    },
                    "text": null,
                    "date": null,
                    "dropDownOption": {
                        "uri": null,
                        "name": dag_run.conf["prevailingwages"]
                    },
                    "number": null
                }
            ],
            "resourceAssignmentModifications": null,
            "resourceProjectAssignmentModifications": null,
            "billingContractModifications": null,
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": [],
            "portfolioToApply": null,
            "locationToApply": null,
            "divisionToApply": null,
            "serviceCenterToApply": null,
            "costCenterToApply": null,
            "departmentGroupToApply": null,
            "employeeTypeGroupToApply": null
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }


def get_custom_fields_prevailing_wage(wage_uri, wage_rate):

    return {
        "customField": {
            "uri": wage_uri,
            "name": null,
            "groupUri": null
        },
        "text": null,
        "date": null,
        "dropDownOption": {
            "uri": null,
            "name": null
        },
        "number": wage_rate.strip()
    }


def get_prevailing_wage_task_request(dag_run):
    return {
        "project": {
            "uri": dag_run.conf["projecturi"],
            "name": null,
            "parameterCorrelationId": null
        },
        "task": {
            "target": {
                "uri": null,
                "name": rail.result("create_task_with_paygroups")["name"],
                "parent": null,
                "parameterCorrelationId": null
            },
            "name": rail.result("create_task_with_paygroups")["name"],
            "code": rail.result("create_task_with_paygroups")["paygroup"],
            "description": null,
            "timeEntryDateRange": null,
            "percentCompleted": "0",
            "isTimeEntryAllowed": "1",
            "estimatedHours": null,
            "isClosed": "0",
            "customFieldValues": [
                get_custom_fields_prevailing_wage(dag_run.conf["Prevailing wages RT uri"], rail.result(
                    "create_task_with_paygroups")["rate1"]),
                get_custom_fields_prevailing_wage(dag_run.conf["Prevailing wages OT uri"], rail.result(
                    "create_task_with_paygroups")["rate2"]),
                get_custom_fields_prevailing_wage(dag_run.conf["Prevailing wages DT uri"], rail.result(
                    "create_task_with_paygroups")["rate3"])
            ],
            "estimatedCost": null,
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": null,
            "assignedResources": [
                {
                    "uri": null,
                    "resourcePlaceholderParameterCorrelationId": null,
                    "user": null,
                    "department": {
                        "uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1",
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "placeholder": null,
                    "location": null,
                    "division": null,
                    "costCenter": null,
                    "serviceCenter": null,
                    "departmentGroup": null,
                    "employeeTypeGroup": null
                }
            ]
        }
    }


def get_basic_task_request(dag_run):
    return {
        "project": {
            "uri": dag_run.conf["projecturi"],
            "name": null,
            "parameterCorrelationId": null
        },
        "task": {
            "target": {
                "uri": null,
                "name": rail.result("create_task_with_basic_task")["costcodename"],
                "parent": null,
                "parameterCorrelationId": null
            },
            "name": rail.result("create_task_with_basic_task")["costcodename"],
            "code": rail.result("create_task_with_basic_task")["costcode"],
            "description": null,
            "timeEntryDateRange": null,
            "percentCompleted": "0",
            "isTimeEntryAllowed": "1",
            "estimatedHours": null,
            "isClosed": "0",
            "customFieldValues": [],
            "estimatedCost": null,
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": null,
            "assignedResources": [
                {
                    "uri": null,
                    "resourcePlaceholderParameterCorrelationId": null,
                    "user": null,
                    "department": {
                        "uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1",
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "placeholder": null,
                    "location": null,
                    "division": null,
                    "costCenter": null,
                    "serviceCenter": null,
                    "departmentGroup": null,
                    "employeeTypeGroup": null
                }
            ]
        }
    }

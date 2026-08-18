from datetime import datetime
from uuid import uuid4
import rail
null = None


def get_replicon_date(date_str):
    date_val = datetime.strptime(date_str, "%m/%d/%Y")
    return {
        "year": date_val.year,
        "month": date_val.month,
        "day": date_val.day
    }


def get_create_project_request(dag_run):
    return {
        "target": null,
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf["Project Name"]
            },
            "codeToApply": {
                "value": dag_run.conf["Project Code"]
            } if dag_run.conf["Project Code"] else null,
            "descriptionToApply": {
                "value": dag_run.conf["Project Description"]
            } if dag_run.conf["Project Description"] else null,
            "percentCompletedToApply": int(dag_run.conf["Percent Completed"]) if dag_run.conf["Percent Completed"] else null,
            "startDateToApply": {
                "date": get_replicon_date(dag_run.conf["Start Date"])
            } if dag_run.conf["Start Date"] else null,
            "endDateToApply": {
                "date": get_replicon_date(dag_run.conf["End Date"])
            } if dag_run.conf["End Date"] else null,
            "billingTypeToApply": {
                "value": "urn:replicon:billing-type:time-and-material",
            },
            "clientBillingAllocationMethodToApply": null,
            "clientAssignmentsSchedulesToApply": null,
            "statusToApply": {
                "name": dag_run.conf["Status"]
            },
            "projectWorkflowStateToApply": null,
            "clientRepresentativeToApply": null,
            "programToApply": {
            "program": {
                "uri": null,
                "name": dag_run.conf["Program Name"]
            }
            } if dag_run.conf["Program Name"] else null,
            "projectLeaderToApply": {
                "user": {
                    "uri": null,
                    # pylint:disable=line-too-long
                    "loginName": (dag_run.conf["Project Manager"].split(",")[1]).strip().lower() + "." + (dag_run.conf["Project Manager"].split(",")[0]).strip().lower(),
                    "employeeId": null,
                    "parameterCorrelationId": null
                },
            }if dag_run.conf["Project Manager"] else null,
            "isProjectLeaderApprovalRequired": "true"if dag_run.conf["Project Leader Approval Required"] == "Yes" else "false",
            "costTypeToApply": null,
            "isTimeEntryAllowed": "false" if dag_run.conf["Allow Time Entry"] == "Yes" else "true",
            "estimatedHoursToApply": {
                "duration": {
                    "hours": int(dag_run.conf["Estimated Hrs"]),
                    "minutes": "0",
                    "seconds": "0"
                }
            } if dag_run.conf["Estimated Hrs"] else null,
            "budgetedHoursToApply": null,
            "estimatedCostToApply": {
                "value": {
                    "amount": dag_run.conf["Estimated Cost Amount"],
                    "currency": {
                        "symbol": dag_run.conf["Estimated Cost Currency"]
                    }
                }
            } if dag_run.conf["Estimated Cost Amount"] else null,
            "budgetedCostToApply": null,
            "expenseBudgetedCostToApply": null,
            "totalEstimatedContractValueToApply": null,
            "defaultBillingCurrencyToApply": {
                "currency": {
                    "symbol": dag_run.conf["Invoice Currency"]
                }
            } if dag_run.conf["Invoice Currency"] else null,
            "timeAndMaterials": {
                "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
            },
            "billingContractToApply": null,
            "fixedBid": null,
            "customFieldsToApply": [],
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


def get_task_details(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:task-list-column:task",
            "urn:replicon:task-list-column:full-path",
            "urn:replicon:task-list-column:parent"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:task-list-filter:project"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": rail.result("get_project_details"),
                        "uris": [],
                        "bool": null,
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
                    "filterDefinitionUri": "urn:replicon:task-list-filter:text"
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
                        "text": dag_run.conf["Task Name Level 1"],
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


def get_create_task_request(dag_run):
    return {
        "target": null,
        "project": {
            "uri": rail.result("get_project_details"),
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": dag_run.conf["Task Name Level 1"],
            "codeToApply": {
                "value": dag_run.conf["Task Code"]
            } if dag_run.conf["Task Code"] else null,
            "descriptionToApply": {
                "value": dag_run.conf["Task Description"]
            } if dag_run.conf["Task Description"] else null,
            "isClosed": "false",
            "timeEntryStartDateToApply": {
                "date": get_replicon_date(dag_run.conf["Task Start Date"])
            } if dag_run.conf["Task Start Date"] else null,
            "timeEntryEndDateToApply": {
                "date": get_replicon_date(dag_run.conf["Task End Date"])
            } if dag_run.conf["Task End Date"] else null,
            "timeAndExpenseEntryTypeToApply": {
                "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
            },
            "isTimeEntryAllowed": "true" if dag_run.conf["Is Time Entry Allowed"] else "false",
            "costTypeToApply": null,
            "estimatedHoursToApply": {
                "duration": {
                    "hours": int(dag_run.conf["Task Estimated Hours"]),
                    "minutes": "0",
                    "seconds": "0"
                }
            } if dag_run.conf["Task Estimated Hours"] else null,
            "estimatedCostToApply": {
                "value": {
                    "amount": dag_run.conf["Estimated Cost Amount"],
                    "currency": {
                        "uri": null,
                        "name": null,
                        "symbol": dag_run.conf["Estimated Cost Currency"]
                    }
                }
            } if dag_run.conf["Estimated Cost Amount"] else null,
            "resourceAssignmentModifications": null,
            "resourceTaskAssignmentModifications": null,
            "customFieldsToApply": [],
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": []
        },
        "unitOfWorkId": str(uuid4())
    }


def get_create_child_task_request(dag_run):
    return {
        "target": null,
        "project": {
            "uri": rail.result("get_project_details"),
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": dag_run.conf["Task Name Level 1"] + "|" + dag_run.conf["Task Name Level 2"],
            "codeToApply": {
                "value": dag_run.conf["Task Code"]
            } if dag_run.conf["Task Code"] else null,
            "descriptionToApply": {
                "value": dag_run.conf["Task Description"]
            } if dag_run.conf["Task Description"] else null,
            "isClosed": "false",
            "timeEntryStartDateToApply": {
                "date": get_replicon_date(dag_run.conf["Task Start Date"])
            } if dag_run.conf["Task Start Date"] else null,
            "timeEntryEndDateToApply": {
                "date": get_replicon_date(dag_run.conf["Task End Date"])
            } if dag_run.conf["Task End Date"] else null,
            "timeAndExpenseEntryTypeToApply": {
                "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
            },
            "isTimeEntryAllowed": "true" if dag_run.conf["Is Time Entry Allowed"] else "false",
            "costTypeToApply": null,
            "estimatedHoursToApply": {
                "duration": {
                    "hours": int(dag_run.conf["Task Estimated Hours"]),
                    "minutes": "0",
                    "seconds": "0"
                }
            } if dag_run.conf["Task Estimated Hours"] else null,
            "estimatedCostToApply": {
                "value": {
                    "amount": dag_run.conf["Estimated Cost Amount"],
                    "currency": {
                        "uri": null,
                        "name": null,
                        "symbol": dag_run.conf["Estimated Cost Currency"]
                    }
                }
            } if dag_run.conf["Estimated Cost Amount"] else null,
            "resourceAssignmentModifications": null,
            "resourceTaskAssignmentModifications": null,
            "customFieldsToApply": [],
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": []
        },
        "unitOfWorkId": str(uuid4())
    }


def get_user_request():
    return {
        "page": "1",
        "pagesize": "10",
        "columnUris": [
            "urn:replicon:user-list-column:user"
        ],
        "sort": [],
        "filterExpression": {
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
                    "text": rail.result("add_users_to_task").split(",")[-1],
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

import rail
import uuid
from datetime import datetime
null = None

def get_project_details_payload(dag_run):
    return {
        "projects": [
            {
                "name":  dag_run.conf["clientName"]
            }
        ]
    }


def create_project_copy_batch_payload(dag_run, date_format):
    return {
        "copyParameter": {
            "sourceProject": {
                "name": "Project template",
            },
            "destinationProjectInfo": {
                "name": dag_run.conf["clientName"],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["job_run_date"], date_format)
                    },
                    "statusLabel": {
                        "name": "In Progress"
                    },
                    "clients": [],
            },
            "taskCopyOptionUri": "urn:replicon:project-copy-task-copy-option:copy",
            "teamCopyOptionUri": "urn:replicon:project-copy-team-copy-option:do-not-copy",
            "billingRateCopyOptionUri": "urn:replicon:project-copy-billing-rate-copy-option:copy-from-project",
            "expenseCodeCopyOptionUri": "urn:replicon:project-copy-expense-code-copy-option:do-not-copy",
            "taskDateCopyOptionUri": "urn:replicon:task-date-copy-option:shift-by-project-start-date-offset",
            "rateTableEntryCopyOptionUri": "urn:replicon:rate-table-entry-copy-option:do-not-copy",
            "billingContractCopyOptionUri": "urn:replicon:billing-contract-copy-option:do-not-copy",
            "shiftDatesByProjectStartDateOffset": "true"
        }
    }


def bulk_get_resource_assignments(dag_run, date_format):
    return {
          "taskUris": [
              rail.result("task_level_2_uri")
          ],
          "asOfDate": rail.parse_date(dag_run.conf["job_run_date"], date_format)
    }

def add_task_level_1_payload(dag_run, date_format):
    return {
        "project": {
            "uri":rail.result("project_uri_to_pass"),
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "task": {
            "target": {
                "uri": null,
                "name": dag_run.conf["customDirectIndirect"],
                "parent": null,
                "parameterCorrelationId": null
            },
            "name": dag_run.conf["customDirectIndirect"],
            "code": null,
            "description": null,
            "timeEntryDateRange": {
                "startDate":rail.parse_date(dag_run.conf["job_run_date"], date_format),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "percentCompleted": "0",
            "isTimeEntryAllowed": "true",
            "estimatedHours": null,
            "isClosed": "false",
            "customFieldValues": [],
            "estimatedCost": {
                "amount": "0",
                "currency": {
                    "uri": null,
                    "name": null,
                    "symbol": "USD$"
                }
            },
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
            "assignedResources": [],
            "keyValues": []
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def add_task_level_2_payload(dag_run, task_level_1_uri, date_format):
    return {
        "project": {
            "uri": rail.result("project_uri_to_pass")
        },
        "task": {
            "target": {
                "uri": null,
                "name": dag_run.conf["projectName"],
                "parent": {
                    "uri": task_level_1_uri,
                    "name": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "parameterCorrelationId": null
            },
            "name": dag_run.conf["projectName"],
            "code": null,
            "description": null,
            "timeEntryDateRange": {
                "startDate": rail.parse_date(dag_run.conf["job_run_date"], date_format),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "percentCompleted": "0",
            "isTimeEntryAllowed": "true",
            "estimatedHours": null,
            "isClosed": "false",
            "customFieldValues": [],
            "estimatedCost": {
                "amount": "0",
                "currency": {
                    "uri": null,
                    "name": null,
                    "symbol": "USD$"
                }
            },
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
            "assignedResources": [],
            "keyValues": []
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def add_default_task_level_2_payload(item, dag_run, date_format):
    return {
        "project": {
            "uri": rail.result("project_uri_to_pass"),
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "task": {
            "target": {
                "uri": null,
                "name": item['task_level_2'],
                "parent": {
                    "uri": rail.result("add_task_level_1")['uri'],
                    "name": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "parameterCorrelationId": null
            },
            "name": item['task_level_2'],
            "code": null,
            "description": null,
            "timeEntryDateRange": {
                "startDate":rail.parse_date(dag_run.conf["job_run_date"], date_format),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "percentCompleted": "0",
            "isTimeEntryAllowed": "true",
            "estimatedHours": null,
            "isClosed": "false",
            "customFieldValues": [],
            "estimatedCost": {
                "amount": "0",
                "currency": {
                    "uri": null,
                    "name": null,
                    "symbol": "USD$"
                }
            },
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
            "assignedResources": [],
            "keyValues": []
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


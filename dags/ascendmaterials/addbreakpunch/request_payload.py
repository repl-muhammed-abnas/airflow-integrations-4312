import uuid
from ascendmaterials.addbreakpunch.custom_methods import get_current_date, get_punchtime

null=None

def time_punch_data_request():
    return {
        "page": "1",
        "pagesize": "200000000",
        "columnUris": [
            "urn:replicon:time-punch-list-column:user",
            "urn:replicon:time-punch-list-column:date-time",
            "urn:replicon:time-punch-list-column:time-punch-action",
            "urn:replicon:time-punch-list-column:time-punch",
            "urn:replicon:time-punch-list-column:activity"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:time-punch-list-filter:time-punch-date-time"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
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
                "text": null,
                "time": null,
                "calendarDayDurationValue": null,
                "workdayDurationValue": null,
                "dateRange": {
                "startDate": get_current_date(),
                "endDate": get_current_date(),
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
                },
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

def shift_details_request():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
        "urn:replicon:shift-assignment-list-column:user",
        "urn:replicon:shift-assignment-list-column:shift"
        ],
        "sort": [],
        "filterExpression": {
        "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:shift-assignment-list-filter:date"
        },
        "operatorUri": "urn:replicon:filter-operator:in",
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
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": {
                "startDate": get_current_date(),
                "endDate": get_current_date(),
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
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

def get_activity_data(dag_run):
    if "activityuri" in dag_run.conf and dag_run.conf["activityuri"] is not null:
        return {
            "activity": {
                "uri": dag_run.conf["activityuri"],
                "name": null
            },
            "project": null,
            "client": null,
            "task": null,
            "billingRate": null,
            "isBillable": null
        }
    return null

def break_time_punch_entry_request(dag_run):
    return {
                "timePunches": [
                    {
                    "timePunch": {
                        "target": {
                        "parameterCorrelationId": null,
                        "uri": null,
                        "slug": null
                        },
                        "user": {
                        "uri": dag_run.conf["useruri"],
                        "loginName": null,
                        "parameterCorrelationId": null
                        },
                        "punchTime": get_punchtime(dag_run,"start"),
                        "actionUri": "urn:replicon:time-punch-action:start-break",
                        "punchInAttributes": null,
                        "punchStartBreakAttributes": {
                        "breakType": {
                            "uri": dag_run.conf["breakuri"],
                            "name": null
                        }
                        },
                        "extensionFieldValues": [],
                        "rawTimePunchUri": null
                    },
                    "audit": null
                    },
                    {
                    "timePunch": {
                        "target": {
                        "parameterCorrelationId": null,
                        "uri": null,
                        "slug": null
                        },
                        "user": {
                        "uri": dag_run.conf["useruri"],
                        "loginName": null,
                        "parameterCorrelationId": null
                        },
                        "punchTime": get_punchtime(dag_run,"stop"),
                        "actionUri": "urn:replicon:time-punch-action:in",
                        "punchInAttributes": get_activity_data(dag_run),
                        "punchStartBreakAttributes": null,
                        "extensionFieldValues": [],
                        "rawTimePunchUri": null
                    },
                    "audit": null
                    }
                ],
                "bulkPutTimePunchBehaviour": {
                    "bulkPutTimePunchBehaviourErrorHandlingOptionUri":
                    "urn:replicon:bulk-put-time-punch-behaviour-error-handling-option:fault-and-rollback-on-error"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }

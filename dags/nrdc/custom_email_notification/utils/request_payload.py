import rail
null = None

def get_data_notsubmitted_timesheets_payload():
    payload = {
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:timesheet-list-column:timesheet-owner",
                    "urn:replicon:timesheet-list-column:timesheet",
                    "urn:replicon:timesheet-list-column:timesheet-period",
                    "urn:replicon:timesheet-list-column:approval-status",
                    "urn:replicon:timesheet-list-column:due-date"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:timesheet-list-filter:due-date"
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
                            "startDate": rail.result('get_required_due_date'),
                            "endDate": rail.result('get_required_due_date'),
                            "relativeDateRangeUri": null,
                            "relativeDateRangeAsOfDate": null
                        },
                        "dateTimeUtc": null
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
                        "filterDefinitionUri": "urn:replicon:timesheet-list-filter:approval-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                        "uri": "urn:replicon:approval-status:open",
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
                        "dateTimeUtc": null
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
    
    return payload
def get_timesheetwaitingforapproval_payload():
    data = '''{
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:timesheet-list-column:timesheet",
            "urn:replicon:timesheet-list-column:timesheet-status",
            "urn:replicon:timesheet-list-column:timesheet-owner",
            "urn:replicon:timesheet-list-column:approval-due-date",
            "urn:replicon:timesheet-list-column:due-date",
            "urn:replicon:timesheet-list-column:timesheet-period",
            "urn:replicon:timesheet-list-column:timesheet-owner",
            "urn:replicon:timesheet-list-column:currently-waiting-on-approver",
            "urn:replicon:timesheet-list-column:supervisor-of-timesheet-owner"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-status"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": {
                "uri": "urn:replicon:timesheet-status:waiting",
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
        }
        }'''
    return data

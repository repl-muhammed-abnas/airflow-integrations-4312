from pendulum import now

null = None


def get_create_time_data_export_batch_payload(time_zone, payload_type='time_export'):
    current_date = now(tz=time_zone)
    end_date = {
        "year": current_date.year,
        "month": current_date.month,
        "day": current_date.day
    }

    # October month logic
    if current_date.strftime("%m") == "10":
        end_date['day'] = 31

    filterexpression = {
        "leftExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
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
                            "startDate": null,
                            "endDate": end_date,
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
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [
                            "urn:replicon:time-data-item-time-data-export-status:none"
                        ],
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
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-approval-status"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [
                        "urn:replicon:approval-status:approved"
                    ],
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
        "value": null,
        "filterDefinitionUri": null
    }

    if payload_type == 'row_count':
        return {
            "columnUris": [
                "urn:replicon:time-data-export-column:user",
                "urn:replicon:time-data-export-column:entry-date"
            ],
            "sort": [],
            "filterExpressions": [filterexpression],
            "fileFormatScriptUri": null
        }

    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": filterexpression,
        "fileFormatScriptUri": null
    }

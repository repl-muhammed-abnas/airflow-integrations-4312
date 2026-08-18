import rail
null = None


def get_all_costcenter_payload():
    payload = {
        "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:cost-center",
                    "urn:replicon:cost-center-list-column:full-path",
                    "urn:replicon:cost-center-list-column:effectively-enabled"
                ],
        "sort": [],
        "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:cost-center-list-filter:effectively-enabled"
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
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
    }
    return payload


def get_report_generate_batch_payload():
    cost_center_list = rail.result('costcenters_list_to_process')
    enabled_filters = rail.result("get_costcenter_report_details")["filterConfiguration"]["enabledFilters"]

    timesheet_period_filter = rail.find_first_by_attr_and_get_attr(enabled_filters, "displayText", "TimesheetPeriodFilter", "uri")

    if not timesheet_period_filter:
        raise Exception("Filter URI not found for filter TimesheetPeriodFilter")

    filter_list_1 = [
        {
            "reportFilterUri": timesheet_period_filter,
            "value": null
        },
        {
            "reportFilterUri": timesheet_period_filter,
            "value": rail.result('get_required_data_for_run').get('timesheet_period_start_date')
        },
        {
            "reportFilterUri": timesheet_period_filter,
            "value": rail.result('get_required_data_for_run').get('timesheet_period_end_date')
        },
    ]
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_costcenter_report_details")["uri"],
                "filterValues": filter_list_1 + cost_center_list,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

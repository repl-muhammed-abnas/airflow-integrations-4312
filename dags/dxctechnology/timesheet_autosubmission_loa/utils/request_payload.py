import rail

null = None

def get_enabled_divisions_company_codes_payload():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": True,
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

def get_filters(dag_run, company_code_values):
    filters = [{
        "reportFilterUri": dag_run.conf['timesheet_period_filter_uri'],
        "value": null
    }, {
        "reportFilterUri": dag_run.conf['timesheet_period_filter_uri'],
        "value": dag_run.conf['report_start_date']
    }, {
        "reportFilterUri": dag_run.conf['timesheet_period_filter_uri'],
        "value": dag_run.conf['report_end_date']
    },
    {
        "reportFilterUri": dag_run.conf['timesheet_period_filter_uri'],
        "value": "Overlapped"
    }
    ]
    for item in dag_run.conf[company_code_values]:
        filters.append({
            "reportFilterUri": dag_run.conf['current_division_filter_uri'],
            "value": item
        })
    return filters


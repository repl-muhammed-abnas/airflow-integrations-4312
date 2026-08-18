import rail

null = None

def get_leave_request_report_filters(dag_run):
    datefilter = rail.find_first_by_attr_and_get_attr(rail.result('get_leave_balance_report_details')['filterConfiguration']['enabledFilters'],
                    'displayText', "AsOfDateFilter", 'uri')
    return [
        {
            "reportFilterUri": datefilter,
            "value": "DateRange"
        },
        {
            "reportFilterUri": datefilter,
            "value": rail.result("logging_details")["export_end_date"]
        },
        {
            "reportFilterUri": datefilter,
            "value": rail.result("logging_details")["export_start_date"]
        }
    ]

def get_report_parameters(dag_run):
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_leave_balance_report_details')["uri"],
                "filterValues": get_leave_request_report_filters(dag_run),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

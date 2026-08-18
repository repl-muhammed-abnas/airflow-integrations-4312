import rail

null = None

def get_leave_request_report_filters(report_details):
    modified_on_datefilter = rail.find_first_by_attr_and_get_attr(report_details['filterConfiguration']['enabledFilters'],
                    'displayText', "ModifiedOnUtcDateRangeFilter", 'uri')
    return [
        {
            "reportFilterUri": modified_on_datefilter,
            "value": null
        },
        {
            "reportFilterUri": modified_on_datefilter,
            "value": rail.result("logging_details")["export_start_date"]
        },
        {
            "reportFilterUri": modified_on_datefilter,
            "value": rail.result("logging_details")["export_end_date"]
        }
    ]

def get_report_parameters(report_details):
    return {
        "reportParameters": [
            {
                "reportUri": report_details["uri"],
                "filterValues": get_leave_request_report_filters(report_details),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_costcenter_hierarchy():
    return {
        "page": "1",
        "pagesize": "10000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:full-path",
            "urn:replicon:cost-center-list-column:cost-center"
        ],
        "sort": [],
        "filterExpression": null
    }

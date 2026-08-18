import rail

null = None

def get_leave_request_report_filters(report_details, modified_on, dag_run):
    modified_on_datefilter = rail.find_first_by_attr_and_get_attr(report_details['filterConfiguration']['enabledFilters'],
                    'displayText', "ModifiedOnUtcDateRangeFilter", 'uri')
    return [
        {
            "reportFilterUri": modified_on_datefilter,
            "value": null
        },
        {
            "reportFilterUri": modified_on_datefilter,
            "value": dag_run.conf["start_date"] if dag_run.conf and dag_run.conf["start_date"] else modified_on
        },
        {
            "reportFilterUri": modified_on_datefilter,
            "value": dag_run.conf["end_date"] if dag_run.conf and dag_run.conf["end_date"] else modified_on
        }
    ]

def get_report_parameters(report_details, modified_on, dag_run):
    return {
        "reportParameters": [
            {
                "reportUri": report_details["uri"],
                "filterValues": get_leave_request_report_filters(report_details, modified_on, dag_run),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

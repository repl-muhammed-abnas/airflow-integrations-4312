import rail

null = None

def get_time_entry_report_filters(dag_run):
    entrydate_filter = rail.find_first_by_attr_and_get_attr(rail.result('get_time_entry_report_details')['filterConfiguration']['enabledFilters'],
                    'displayText', "EntryDateFilter", 'uri')
    return [
        {
            "reportFilterUri": entrydate_filter,
            "value": null
        },
        {
            "reportFilterUri": entrydate_filter,
            "value": dag_run.conf["export_start_date"]
        },
        {
            "reportFilterUri": entrydate_filter,
            "value": dag_run.conf["export_end_date"]
        }
    ]

def get_time_entry_report_parameters(dag_run):
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_time_entry_report_details')["uri"],
                "filterValues": get_time_entry_report_filters(dag_run),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


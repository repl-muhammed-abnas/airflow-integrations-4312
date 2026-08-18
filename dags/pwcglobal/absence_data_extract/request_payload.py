import rail
from pwcglobal.absence_data_extract import custom_method


def get_run_report_payload(time_zone):
    get_specific_report_details = rail.result('get_specific_report_details')

    def get_specific_filter_uri(filter_name):
        return rail.find_first_by_attr_and_get_attr(get_specific_report_details["filterConfiguration"]["enabledFilters"], 'displayText', filter_name, 'uri')

    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_all_reports'),
                "filterValues": [
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="CurrentLocationFilter"),
                        "value": rail.result("get_enabled_locations").split(":")[-1]
                    },
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="TimeEntrySubmissionDateFilter"),
                        "value": None
                    },
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="TimeEntrySubmissionDateFilter"),
                        "value": custom_method.get_report_start_end_date(time_zone)

                    },
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="TimeEntrySubmissionDateFilter"),
                        "value": custom_method.get_report_start_end_date(time_zone),
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

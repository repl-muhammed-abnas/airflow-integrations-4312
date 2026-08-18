from datetime import datetime as dt, timedelta
import pytz
import rail


def get_run_report_payload():
    get_specific_report_details = rail.result('get_specific_report_details')

    def get_specific_filter_uri(filter_name):
        return rail.find_first_by_attr_and_get_attr(
            get_specific_report_details["filterConfiguration"]["enabledFilters"], 'displayText', filter_name, 'uri')

    return {
        "reportParameters": [
            {
                "reportUri": get_specific_report_details['uri'],
                "filterValues": [
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="EntryDateFilter"),
                        "value": None
                    },
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="EntryDateFilter"),
                        "value": str((dt.now(pytz.timezone('US/Pacific')) - timedelta(days=119)).strftime("%m/%d/%Y"))
                    },
                    {
                        "reportFilterUri": get_specific_filter_uri(filter_name="EntryDateFilter"),
                        "value": str(dt.now(pytz.timezone('US/Pacific')).strftime("%m/%d/%Y"))

                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

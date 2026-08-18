from datetime import timedelta
import pendulum
import rail

today = pendulum.now()
startdate = (today.start_of('week') - timedelta(days=8)).strftime("%Y-%m-%d")
enddate = (today.start_of('week') - timedelta(days=2)).strftime("%Y-%m-%d")


def get_report_params():
    entry_date_filter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_report_details')[
            'filterConfiguration']
        ['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri')
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')['uri'],
                "filterValues": [
                    {
                        "reportFilterUri": entry_date_filter,
                        "value": None
                    },
                    {
                        "reportFilterUri": entry_date_filter,
                        "value": startdate
                    },
                    {
                        "reportFilterUri": entry_date_filter,
                        "value": enddate
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

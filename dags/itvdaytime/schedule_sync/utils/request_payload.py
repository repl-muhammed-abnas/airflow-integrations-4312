import rail

null = None


def get_generate_report_payload():
    #pylint: disable=line-too-long
    entry_date_filter = rail.find_first_by_attr_and_get_attr(rail.result("get_report_details")[
                                                             'filterConfiguration']['enabledFilters'], "displayText", "EntryDateFilter", 'uri')
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_report_details")['uri'],
                "filterValues": [
                    {
                        "reportFilterUri": entry_date_filter,
                        "value": null
                    },
                    {
                        "reportFilterUri": entry_date_filter,
                        "value": rail.result('get_required_details')['last_week_monday']
                    },
                    {
                        "reportFilterUri": entry_date_filter,
                        "value": rail.result('get_required_details')['next_third_week_sunday']
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

import rail

def get_report_filter_uri(report_details, filter_name):
    return rail.find_first_by_attr_and_get_attr(
        rail.result(report_details)['filterConfiguration']['enabledFilters'], 'displayText', filter_name, 'uri')

def get_to_params(sd, ed):
    print(get_to_filter_values(sd, ed))
    return {
        "reportParameters": [
                {
                    "reportUri": rail.result('get_to_transaction_report_details')["uri"],
                    "filterValues": get_to_filter_values(sd, ed),
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
            ]
        }

def get_to_filter_values(sd, ed):
    report_filter_uri = get_report_filter_uri('get_to_transaction_report_details', "TimeOffTypeFilter")
    data = rail.result('get_toil_totypes')
    param = list(map(lambda row: {
        "reportFilterUri": report_filter_uri,
        "value": row['value'],
    }, data))

    param1 = [
                {
                    "reportFilterUri": get_report_filter_uri('get_to_transaction_report_details', "DateRangeFilter"),
                    "value": None,
                },
                {
                    "reportFilterUri": get_report_filter_uri('get_to_transaction_report_details', "DateRangeFilter"),
                    "value": sd,
                },
                {
                    "reportFilterUri": get_report_filter_uri('get_to_transaction_report_details', "DateRangeFilter"),
                    "value": ed,
                }
            ]
    return param1 + param

def get_ts_day_params(sd, ed):
    print(get_ts_day_filter_values(sd, ed))
    return {
            "reportParameters": [
                {
                    "reportUri": rail.result('get_ts_day_report_details')['uri'],
                    "filterValues": get_ts_day_filter_values(sd, ed),
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
            ]
        }

def get_ts_day_filter_values(sd, ed):
    location_uri = rail.result("get_enabled_locations").split(":")[-1]
    print("location_uri", location_uri)
    return  [
                {
                    "reportFilterUri": get_report_filter_uri("get_ts_day_report_details", "EntryDateFilter"),
                    "value": None,
                },
                {
                    "reportFilterUri": get_report_filter_uri("get_ts_day_report_details", "EntryDateFilter"),
                    "value": sd,
                },
                {
                    "reportFilterUri": get_report_filter_uri("get_ts_day_report_details", "EntryDateFilter"),
                    "value": ed,
                },
                {
                    "reportFilterUri": get_report_filter_uri("get_ts_day_report_details", "LocationFilter"),
                    "value": location_uri
                }
            ]

def get_user_params():
    return {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_user_report_details')["uri"],
                        "filterValues": [
                            {
                                "reportFilterUri": get_report_filter_uri('get_user_report_details', "LocationFilter"),
                                "value": rail.result("get_enabled_locations").split(":")[-1]
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }

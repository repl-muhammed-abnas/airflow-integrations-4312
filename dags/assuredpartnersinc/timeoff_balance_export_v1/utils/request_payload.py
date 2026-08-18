from datetime import timedelta, date
import rail
import pendulum


def get_as_of_date(users_timeoff_report):
    return " ".join(list(map(lambda enabled_filters: enabled_filters["uri"],
                             filter(lambda enabled_filters: enabled_filters['displayText'] == "AsOfDateFilter",
                                    users_timeoff_report['filterConfiguration']['enabledFilters']))))


def get_enabled_users_batch_payload():
    time_zone = rail.result("get_logging_details")['time_zone']
    as_of_date = get_as_of_date(rail.result(
        'get_enabled_users_timeoff_report'))
    start_date = str(
        (pendulum.now(time_zone)-timedelta(days=14)).strftime("%m/%d/%Y"))
    end_date = str(
        (pendulum.now(time_zone)-timedelta(days=1)).strftime("%m/%d/%Y"))

    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_enabled_users_timeoff_report')['uri'],
                "filterValues": [
                    {
                        "reportFilterUri": as_of_date,
                        "value": "DateRange"
                    },
                    {
                        "reportFilterUri": as_of_date,
                        "value": end_date
                    },
                    {
                        "reportFilterUri": as_of_date,
                        "value": start_date
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_disabled_users_batch_payload():
    time_zone = rail.result("get_logging_details")['time_zone']
    as_of_date = get_as_of_date(rail.result(
        'get_disabled_users_timeoff_report'))
    udf_filter = " ".join(list(map(lambda enabled_filters: enabled_filters["uri"],
                                   filter(lambda enabled_filters: enabled_filters['displayText'] == "UDFFilter_User45_EndDate",
                                          rail.result('get_disabled_users_timeoff_report')['filterConfiguration']['enabledFilters']))))
    report_start_date = str(
        (pendulum.now(time_zone)-timedelta(days=14)).strftime("%m/%d/%Y"))
    report_end_date = str(
        (pendulum.now(time_zone)-timedelta(days=1)).strftime("%m/%d/%Y"))
    end_date = str(
        (pendulum.now(time_zone)-timedelta(days=15)).strftime("%m/%d/%Y"))
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_disabled_users_timeoff_report')['uri'],
                "filterValues": [
                    {
                        "reportFilterUri": as_of_date,
                        "value": "DateRange"
                    },
                    {
                        "reportFilterUri": as_of_date,
                        "value": report_end_date
                    },
                    {
                        "reportFilterUri": as_of_date,
                        "value": report_start_date
                    },
                    {
                        "reportFilterUri": udf_filter,
                        "value": None
                    },
                    {
                        "reportFilterUri": udf_filter,
                        "value": end_date
                    },
                    {
                        "reportFilterUri": udf_filter,
                        "value": None
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

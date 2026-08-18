import functools
import pendulum
import rail

null = None

def get_logging_details(time_zone):
    today = pendulum.now(time_zone)
    return {
        "time_zone": time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    }


def get_file_name(filename_prefix, location, filename_seconds_suffix=null):
    now = pendulum.now()
    if location == 'Europe' and filename_seconds_suffix:
        # For Europe, use hardcoded seconds to avoid filename conflicts
        timestamp = now.strftime('%Y%m%d_%H%M') + filename_seconds_suffix
    else:
        # For other regions, use actual timestamp
        timestamp = now.strftime('%Y%m%d_%H%M%S')
    return f"{filename_prefix}_{timestamp}"

def get_leave_bal_report_filters():
    time_zone = rail.result("logging_details")["time_zone"]
    current_year = pendulum.now(time_zone).year
    datefilter = rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
                    'displayText', "AsOfDateFilter", 'uri')
    start_date = pendulum.datetime(current_year, 1, 1).strftime("%m/%d/%Y")
    end_date = pendulum.now(time_zone).strftime("%m/%d/%Y")
    return [
        {
            "reportFilterUri": datefilter,
            "value": "DateRange"
        },
        {
            "reportFilterUri": datefilter,
            "value": end_date
        },
        {
            "reportFilterUri": datefilter,
            "value": start_date
        }
    ]

def get_report_parameters():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')["uri"],
                "filterValues": get_leave_bal_report_filters(),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def create_batch_creation_datetime(response):
    creation_time = response["creationTime"]
    return pendulum.datetime(creation_time["year"], creation_time["month"], creation_time["day"],
        creation_time["hour"], creation_time["minute"], creation_time["second"]).strftime("%d/%m/%Y %H:%M:%S")

@functools.lru_cache(maxsize=128)
def get_batch_creation_datetime():
    return rail.result("get_batch_creation_time")

def get_csv_row_data(item):
    return [
        item["Employee ID"],
        item["Local Employee Number"],
        item["Time Off Type"],
        item["Time Off Type Description"],
        item["Leave Carry Forward"],
        item["Leave Accrued"],
        item["Leave Availed"],
        item["Leave Reset"],
        item["Leave Balance"],
        item["Units"],
        get_batch_creation_datetime(),
        item["User End Date"]
    ]

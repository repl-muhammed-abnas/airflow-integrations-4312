import pendulum
from rail import get_current_context
import rail

null = None

def get_logging_details(config):
    today = pendulum.now(config.time_zone)
    current_time = today.strftime('%Y%m%d_%H%M%S')
    return {
        "time_zone": config.time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "export_filename": f"{config.filename_prefix}_{current_time}"
    }

def get_dag_run_conf():
    return get_current_context()['dag_run'].conf

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

def get_leave_request_report_filters(modified_on):
    modified_on_datefilter = rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
                    'displayText', "ModifiedOnUtcDateRangeFilter", 'uri')
    return [
        {
            "reportFilterUri": modified_on_datefilter,
            "value": null
        },
        {
            "reportFilterUri": modified_on_datefilter,
            "value": get_dag_run_conf()["start_date"] if get_dag_run_conf() and get_dag_run_conf()["start_date"] else modified_on
        },
        {
            "reportFilterUri": modified_on_datefilter,
            "value": get_dag_run_conf()["end_date"] if get_dag_run_conf() and get_dag_run_conf()["end_date"] else modified_on
        }
    ]

def get_report_parameters(leave_status, modified_on):
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')["uri"],
                "filterValues": get_leave_bal_report_filters() if leave_status == 'leave balance' else get_leave_request_report_filters(modified_on),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_batch_creation_datetime(response):
    creation_time = response["creationTime"]
    return pendulum.datetime(creation_time["year"], creation_time["month"], creation_time["day"],
                creation_time["hour"], creation_time["minute"], creation_time["second"]).strftime("%d/%m/%Y %H:%M:%S")

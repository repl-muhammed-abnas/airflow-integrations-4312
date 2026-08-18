import pendulum
from rail import get_current_context
import rail

null = None

def get_logging_details(time_zone, filename_prefix):
    today = pendulum.now(time_zone)
    current_time = today.strftime('%Y%m%d_%H%M%S')
    return {
        "time_zone": time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "export_filename": f"{filename_prefix}_{current_time}"
    }

def get_dag_run_conf():
    return get_current_context()['dag_run'].conf

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

def get_report_parameters(modified_on):
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')["uri"],
                "filterValues": get_leave_request_report_filters(modified_on),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_leave_data_csv_rows(item, index):
    if not item:
        return []
    return [
        item["Leave_Request_ID"],
        item["Employee_ID"],
        item["Local_Employee_Number"],
        item["Time_Off_Type"],
        item["Time_Off_Type_Description"],
        item["Time_Off_Date"],
        item["Booking_Start_Date"],
        item["Booking_End_Date"],
        item["Time_Off_Days"],
        item["Time_Off_Hours"],
        item["Units"],
        item["Time_Off_Comments"],
        item["Approver_GGID"],
        item["Approval_Status"],
        item["Submitted_By"],
        item["Submitted_On"],
        item["Modified_By"],
        item["Modified_On"],
        item["In_Lieu_Date"],
        item["Reason_for_Special_Leave"],
        item["Reason__PL_"],
        item["_01___Booking_Day__Start_Day_"],
        item["_02___Booking_Day__End_Day_"],
        index
    ]

from datetime import datetime as dt, timedelta
import rail

def get_run_report_payload():
    get_specific_report_details = rail.result('get_specific_report_details')

    def get_specific_filter_uri(filter_name):
        return rail.find_first_by_attr_and_get_attr(
            get_specific_report_details["filterConfiguration"]["enabledFilters"],'displayText', filter_name, 'uri')

    return {
                "reportParameters": [
                {
    "reportUri": get_specific_report_details['uri'],
    "filterValues": [
        {
            "reportFilterUri": get_specific_filter_uri(filter_name = "ApprovalDateFilter"),
            "value": None
        },
        {
            "reportFilterUri": get_specific_filter_uri(filter_name = "ApprovalDateFilter"),
            "value": str((dt.now() - timedelta(days=1)).strftime("%m/%d/%Y"))
        },
        {
            "reportFilterUri": get_specific_filter_uri(filter_name = "ApprovalDateFilter"),
            "value": str((dt.now() - timedelta(days=1)).strftime("%m/%d/%Y"))

        }
    ],
    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
        }
     ]
    }


def get_compose_item_time_off_data_row(items):
    booking_time_format = "%d/%m/%Y"
    booking_start_time= ""
    booking_end_time=""
    if ' - ' in items['Booking_Start_Date_Time'] and items['Booking_Start_Date_Time']:
        booking_start_time = dt.strptime(items['Booking_Start_Date_Time'].split(' - ')[0], booking_time_format).strftime("%Y/%m/%d") + " - "\
         +items['Booking_Start_Date_Time'].split(' - ')[-1]
    else:
        booking_start_time = dt.strptime(items['Booking_Start_Date_Time'].split(' - ')[0], booking_time_format).strftime("%Y/%m/%d")
    if ' - ' in items['Booking_End_Date_Time'] and items['Booking_End_Date_Time']:
        booking_end_time = dt.strptime(items['Booking_End_Date_Time'].split(' - ')[0], booking_time_format).strftime("%Y/%m/%d") + " - "\
         +items['Booking_End_Date_Time'].split(' - ')[-1]
    else:
        booking_end_time = dt.strptime(items['Booking_End_Date_Time'].split(' - ')[0], booking_time_format).strftime("%Y/%m/%d")

    return [items['Employee_ID'],
                items['User_Name'],
                items['Time_Off_Type'],
                items['Absence_Entry_ID_'].split(':')[-1],
                dt.strptime(items['Booking_Start_Date'], booking_time_format).strftime("%Y/%m/%d") if items['Booking_Start_Date'] else "",
                booking_start_time,
                dt.strptime(items['Booking_End_Date'], booking_time_format).strftime("%Y/%m/%d") if items['Booking_End_Date'] else "",
                booking_end_time,
                items['Time_Off_Hrs'],
                items['Approval_Status']]

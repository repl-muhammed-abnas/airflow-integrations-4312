from datetime import datetime
import pendulum
import rail
null = None

REPORT_DATE_FORMAT = "%d/%m/%Y"
EXPORT_DATE_FORMAT = "%Y-%m-%d"

def get_logging_details(time_zone, filename_prefix):
    today = pendulum.now(time_zone)
    current_time = today.strftime('%Y%m%d%H%M%S')
    return {
        "time_zone": time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "export_filename": f"{filename_prefix}_{current_time}"
    }

def get_date_in_format(date_string, date_format, output_date_format):
    return datetime.strptime(date_string, date_format).strftime(output_date_format)

def get_approved_leave_data_rows(item, timeoff_codes):
    booking_days = float(item["booking_days"].replace(",", ""))
    return [
        timeoff_codes[item["timeoff_type"]],
        item["employee_id"].zfill(8),
        get_date_in_format(item["booking_start_date"], REPORT_DATE_FORMAT, EXPORT_DATE_FORMAT),
        get_date_in_format(item["booking_end_date"], REPORT_DATE_FORMAT, EXPORT_DATE_FORMAT),
        item["booking_day_startday"] if item["booking_day_startday"] == "Afternoon" else "",
        item["booking_day_startday"] if booking_days <= 0 and item["booking_day_startday"] == "Morning"
            else (item["booking_day_endday"] if booking_days > 0 and item["booking_day_endday"] == "Morning" else ""),
        item["timeoff_hours"] if item["timeoff_type"] == "[FRA] D - Grève" else "",
        item["booking_uri"].split(":")[-1],
        "Creation",
        "",
        "",
        ""
    ]

def get_deleted_leave_data_rows(item, timeoff_codes):
    return [
        timeoff_codes[item["timeoff_type"]],
        item["employee_id"].zfill(8),
        get_date_in_format(item["booking_start_date"], REPORT_DATE_FORMAT, EXPORT_DATE_FORMAT),
        get_date_in_format(item["booking_end_date"], REPORT_DATE_FORMAT, EXPORT_DATE_FORMAT),
        "",
        "",
        item["timeoff_hours"] if item["timeoff_type"] == "[FRA] D - Grève" else "",
        item["booking_uri"].split(":")[-1],
        "Cancellation",
        "",
        "",
        ""
    ]

def get_empty_export_row():
    return [
        {
            "paycode": "",
            "employee_id": "",
            "booking_start_date": "",
            "booking_end_date": "",
            "day_start_indicator": "",
            "day_end_indicator": "",
            "hours": "",
            "short_id": "",
            "transaction_type": "",
            "horodatage": "",
            "initialorextension": "",
            "workedstartday": ""
        }
    ]

def get_query_to_merge_artifacts(tenant_wide_log_list):
    return " UNION ALL ".join([f"SELECT * FROM tenant_wide_log_data_{idx}" for idx in range(len(tenant_wide_log_list))])

def get_query_to_merge_leaves_data():
    if rail.result("final_approved_leaves") and rail.result("final_deleted_leaves"):
        return "SELECT * FROM final_approved_leaves UNION ALL SELECT * FROM final_deleted_leaves"
    if rail.result("final_approved_leaves") and not rail.result("final_deleted_leaves"):
        return "SELECT * FROM final_approved_leaves"
    if not rail.result("final_approved_leaves") and rail.result("final_deleted_leaves"):
        return "SELECT * FROM final_deleted_leaves"
    return null

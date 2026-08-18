from datetime import datetime
import pendulum
from capgemini.france_leave_data_export_to_sopra.other_leaves_export_v1.mapper.timeoff_codes import timeoff_codes_list
import rail
null = None

REPORT_DATE_FORMAT = "%d/%m/%Y"
EXPORT_DATE_FORMAT = "%Y-%m-%d"

def get_logging_details(config):
    today = pendulum.now(config.time_zone)
    current_time = today.strftime('%Y%m%d%H%M%S')
    return {
        "time_zone": config.time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "export_filename": f"{config.filename_prefix}_{current_time}"
    }

def get_date_in_format(date_string, date_format, output_date_format):
    return datetime.strptime(date_string, date_format).strftime(output_date_format)

def get_approved_leave_data_rows(item):
    booking_days = float(item["booking_days"].replace(",", ""))
    return [
        rail.find_first_by_attr_and_get_attr(timeoff_codes_list, "replicon_time_off_type", item["timeoff_type"], "SOPRA_pay_code", ""),
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

def get_deleted_leave_data_rows(item):
    return [
        rail.find_first_by_attr_and_get_attr(timeoff_codes_list, "replicon_time_off_type", item["timeoff_type"], "SOPRA_pay_code", ""),
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

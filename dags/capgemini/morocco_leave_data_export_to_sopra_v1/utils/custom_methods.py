from datetime import datetime, timedelta
import pendulum
import rail
null = None

REPORT_DATE_FORMAT = "%d/%m/%Y"
EXPORT_DATE_FORMAT = "%Y-%m-%d"

def get_conf():
    return rail.get_current_context()['dag_run'].conf

def get_logging_details(time_zone, ma01_filename_prefix, ma02_ma03_filename_prefix, schedules):
    today = pendulum.now(time_zone)
    current_time = today.strftime('%Y%m%d%H%M%S')
    return {
        "time_zone": time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "ma01_export_filename": f"{ma01_filename_prefix}_{current_time}",
        "ma02_ma03_export_filename": f"{ma02_ma03_filename_prefix}_{current_time}",
        "export_start_date": get_conf()["start_date"] if get_conf() and get_conf()["start_date"] else (
                datetime.strptime(schedules[schedules.index(today.strftime("%d/%m/%Y")) - 1], "%d/%m/%Y")).strftime("%m/%d/%Y"),
        "export_end_date": (get_conf()["end_date"] if get_conf() and get_conf()["end_date"] else
                    (today - timedelta(days=1)).strftime("%m/%d/%Y"))
    }

def get_date_in_format(date_string, date_format, output_date_format):
    return datetime.strptime(date_string, date_format).strftime(output_date_format)

def get_formatted_data(data):
    return float(data.replace(",", "")) if data else null

def get_approved_leave_data_rows(item, timeoff_codes):
    booking_days = float(item["booking_days"].replace(",", ""))
    return [
        timeoff_codes[item["timeoff_type"]],
        item["employee_id"].zfill(8),
        get_date_in_format(item["booking_start_date"], REPORT_DATE_FORMAT, EXPORT_DATE_FORMAT),
        get_date_in_format(item["booking_end_date"], REPORT_DATE_FORMAT, EXPORT_DATE_FORMAT),
        item["half_day_startday"] if item["half_day_startday"] == "Afternoon" else "",
        item["half_day_startday"] if booking_days <= 0 and item["half_day_startday"] == "Morning"
            else (item["half_day_endday"] if booking_days > 0 and item["half_day_endday"] == "Morning" else ""),
        get_formatted_data(item["timeoff_hours"]),
        item["booking_uri"].split(":")[-1],
        "Creation",
        "",
        "",
        "",
        item["cost_center"]
    ]

def get_deleted_leave_data_rows(item, timeoff_codes):
    return [
        timeoff_codes[item["timeoff_type"]],
        item["employee_id"].zfill(8),
        get_date_in_format(item["booking_start_date"], REPORT_DATE_FORMAT, EXPORT_DATE_FORMAT),
        get_date_in_format(item["booking_end_date"], REPORT_DATE_FORMAT, EXPORT_DATE_FORMAT),
        "",
        "",
        get_formatted_data(item["timeoff_hours"]),
        item["booking_uri"].split(":")[-1],
        "Cancellation",
        "",
        "",
        "",
        item["cost_center"]
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

def filter_costcenter_hierarchy(response, config):
    costcenter_data=list(map(lambda costcenter_cells: {
            "costcenter": costcenter_cells["cells"][1]["textValue"],
            "fullpath": " / ".join([costcenter_data["textValue"]
                for costcenter_data in costcenter_cells["cells"][0]["cellCollection"]])
        }, response["rows"]))
    filtered_costcenter = list(filter(lambda costcenter_data:
        config.ma01_costcenter in costcenter_data["fullpath"] or
        config.ma02_costcenter in costcenter_data["fullpath"] or
        config.ma03_costcenter in costcenter_data["fullpath"], costcenter_data))
    return filtered_costcenter

from datetime import datetime
import pendulum
from dateutil.relativedelta import relativedelta
import rail
null = None

REPORT_DATE_FORMAT = "%d/%m/%Y"
EXPORT_DATE_FORMAT = "%Y-%m-%d"

def get_conf():
    return rail.get_current_context()['dag_run'].conf

def get_logging_details(time_zone, ma01_filename_prefix, ma02_ma03_filename_prefix):
    today = pendulum.now(time_zone)
    current_time = today.strftime('%Y%m%d%H%M%S')
    return {
        "time_zone": time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "ma01_export_filename": f"{ma01_filename_prefix}_{current_time}",
        "ma02_ma03_export_filename": f"{ma02_ma03_filename_prefix}_{current_time}",
        "export_start_date": (get_conf()["start_date"] if get_conf() and get_conf()["start_date"] else
            (today - relativedelta(day=1, month=1)).strftime("%m/%d/%Y")),
        "export_end_date": (get_conf()["end_date"] if get_conf() and get_conf()["end_date"] else
            (today + relativedelta(day=31)).strftime("%m/%d/%Y"))
    }

def get_date_in_format(date_string, date_format, output_date_format):
    return datetime.strptime(date_string, date_format).strftime(output_date_format)

def get_formatted_data(data):
    return float(data.replace(",", ""))

def get_leave_balance_data_rows(item, timeoff_codes_list):
    return [
        timeoff_codes_list[item["timeoff_type"]],
        item["employee_id"].zfill(8),
        get_formatted_data(item["current_year_balance"]),
        get_formatted_data(item["leaves_availed"]),
        get_formatted_data(item["leave_balance"]),
        "",
        "",
        item["cost_center_fullpath"]
    ]

def get_empty_export_row():
    return [
        {
            "paycode": "",
            "employee_id": "",
            "current_year_balance": "",
            "leaves_availed": "",
            "leave_balance": "",
            "transaction_type": "",
            "horodatage": ""
        }
    ]

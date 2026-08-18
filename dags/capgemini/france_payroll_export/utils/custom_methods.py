from datetime import datetime
import functools
from dateutil.relativedelta import relativedelta
import pendulum
null = None

REP_DATE_FORMAT = "%Y/%m/%d"
SOPRA_EXPORT_DATE_FORMAT = "%Y-%m-%d"
GFS_EXPORT_DATE_FORMAT = "%d%m%Y"
FILENAME_DATE_FORMAT = "%Y%m%d"

def get_date_json(date_obj):
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }

def get_export_date_range_details(time_zone, filename_prefix):
    today = pendulum.now(time_zone)
    current_time = today.strftime('%Y%m%d%H%M%S')

    current_month_start = today - relativedelta(day=1)
    current_month_end = today + relativedelta(day=31)
    previous_5_months_start = today - relativedelta(months=5, day=1)
    previous_5_months_end = today - relativedelta(months=1, day=31)

    current_month_details = {
        "current_time": today.strftime('%Y%m%d_%H%M%S'),
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "export_start_date": current_month_start.strftime(REP_DATE_FORMAT),
        "export_end_date": current_month_end.strftime(REP_DATE_FORMAT),
        "export_start_date_json": get_date_json(current_month_start),
        "export_end_date_json": get_date_json(current_month_end),
        "sopra_export_filename": f"{filename_prefix}_{current_month_start.strftime(FILENAME_DATE_FORMAT)}_{current_month_end.strftime(FILENAME_DATE_FORMAT)}_FRA_{current_time}",
        "gfs_export_filename": f"GTM_{filename_prefix}_{current_month_start.strftime(FILENAME_DATE_FORMAT)}_{current_month_end.strftime(FILENAME_DATE_FORMAT)}_FRA_PREMIUMS_{current_time}",
        "payroll_name": f"France_Export_{current_month_start.strftime(FILENAME_DATE_FORMAT)}_{current_month_end.strftime(FILENAME_DATE_FORMAT)}_{current_time}"
    }
    previous_5_months_details = {
        "current_time": today.strftime('%Y%m%d_%H%M%S'),
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "export_start_date": previous_5_months_start.strftime(REP_DATE_FORMAT),
        "export_end_date": previous_5_months_end.strftime(REP_DATE_FORMAT),
        "export_start_date_json": get_date_json(previous_5_months_start),
        "export_end_date_json": get_date_json(previous_5_months_end),
        "sopra_export_filename": f"{filename_prefix}_{previous_5_months_start.strftime(FILENAME_DATE_FORMAT)}_{previous_5_months_end.strftime(FILENAME_DATE_FORMAT)}_FRA_{current_time}",
        "gfs_export_filename": f"GTM_{filename_prefix}_{previous_5_months_start.strftime(FILENAME_DATE_FORMAT)}_{previous_5_months_end.strftime(FILENAME_DATE_FORMAT)}_FRA_PREMIUMS_{current_time}",
        "payroll_name": f"France_Export_{previous_5_months_start.strftime(FILENAME_DATE_FORMAT)}_{previous_5_months_end.strftime(FILENAME_DATE_FORMAT)}_{current_time}"
    }

    return [current_month_details, previous_5_months_details]

def get_sopra_payroll_data_rows(item):
    return [
        item["Pay_Code_Code"],
        item["Employee_ID"].zfill(8),
        "Q",
        item["Pay_Code_Hours"],
        "",
        datetime.strptime(item["Entry_Date"], REP_DATE_FORMAT).strftime(SOPRA_EXPORT_DATE_FORMAT),
        ""
    ]

def get_empty_sopra_export_row():
    return [
        {
            "paycode": "",
            "employee_id": "",
            "format": "",
            "hours": "",
            "monsal": "",
            "entrydate": "",
            "entitlement": ""
        }
    ]

@functools.lru_cache(maxsize=128)
def get_payrun_export_uri(dag_run):
    return (dag_run.conf["payrunuri"]).split(":")[-1]

def get_gfs_payroll_data_rows(item, dag_run, index, current_time,config):
    pay_code_code = item["Pay_Code_Code"]
    if item["Desired_Paycode"]:
        pay_code_code = config.desired_paycodes[item["Desired_Paycode"]]
    return [
        f'{current_time}_{pay_code_code}_{index}',
        "GTM_Replicon_Timesheets",
        get_payrun_export_uri(dag_run),
        item["Employee_ID"],
        item["Local_Employee_Number"].zfill(8) if item["Local_Employee_Number"] else "",
        datetime.strptime(item["Entry_Date"], REP_DATE_FORMAT).strftime(GFS_EXPORT_DATE_FORMAT),
        pay_code_code,
        item["Project_Code"],
        item["Task_Code"],
        "",
        "",
        "",
        item["Cost_Center_Code"],
        item["Pay_Code_Hours"],
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        ""
    ]

def get_location_uri(response, location):
    return list(map(lambda locations_data: locations_data["location"]["uri"],
        filter(lambda locations_data: locations_data["location"]["displayText"] == location
            and str(locations_data["hierarchyLevel"]) == "0", response)))[0]

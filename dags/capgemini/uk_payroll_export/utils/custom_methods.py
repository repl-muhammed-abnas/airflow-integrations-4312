from datetime import datetime
from dateutil.relativedelta import relativedelta
import pendulum
import rail
null = None

REPORT_DATE_FORMAT = "%d/%m/%Y"
EXPORT_DATE_FORMAT = "%Y-%m-%d"


def get_date_json(date_obj):
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }


def get_logging_details(time_zone):
    today = pendulum.now(time_zone)
    prev_4_months_date = today - relativedelta(months=4, day=1)
    prev_month_end_date = today - relativedelta(months=1, day=31)
    current_time = today.strftime('%Y%m%d_%H%M%S')
    return {
        "time_zone": time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "export_start_date": prev_4_months_date.strftime("%Y/%m/%d"),
        "export_end_date": prev_month_end_date.strftime("%Y/%m/%d"),
        "export_start_date_json": get_date_json(prev_4_months_date),
        "export_end_date_json": get_date_json(prev_month_end_date),
        "payroll_name_suffix": f"UK_Payroll_{current_time}",
        "oncall_export_filename_suffix": f"UK_OnCall_{current_time}",
        "overtime_export_filename_suffix": f"UK_OT_{current_time}"
    }


def get_overtime_payroll_data_rows(item):
    return [
        item["Local_Employee_Number"],
        item["Cost_Center_Code"],
        item["User"],
        item["User"].split(",")[-1].strip()[0],
        item["Pay_Code_Code"],
        item["Pay_Code_Hours"],
        datetime.strptime(item["Entry_Date"], "%Y/%m/%d").strftime("%y-%b"),
        ""
    ]


def get_oncall_payroll_data_rows(item):
    return [
        item["Local_Employee_Number"],
        datetime.strptime(item["Entry_Date"], "%Y/%m/%d").strftime("%Y-%m-%d"),
        item["Cost_Center_Code"],
        item["User"],
        item["User"].split(",")[-1].strip()[0],
        item["Pay_Code_Hours"]
    ]

def get_uk_location_uri(response, uk_location):
    return list(map(lambda locations_data: locations_data["location"]["uri"],
        filter(lambda locations_data: locations_data["location"]["displayText"] == uk_location
            and str(locations_data["hierarchyLevel"]) == "0", response)))[0]

def get_parent_costcenter_uri(response, dag_run):
    return rail.find_first_by_attr_and_get_attr(response, "costCenter.displayText", dag_run.conf["cost_center_name"], "costCenter.uri")

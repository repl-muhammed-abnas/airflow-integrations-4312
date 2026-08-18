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

def get_logging_details(time_zone, ma01_filename_prefix, ma02_ma03_filename_prefix):
    today = pendulum.now(time_zone)
    prev_3_months_date = today - relativedelta(months=3, day=1)
    current_month_end_date = today + relativedelta(day=31)
    current_time = today.strftime('%Y%m%d%H%M%S')
    return {
        "time_zone": time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "export_start_date": prev_3_months_date.strftime("%Y/%m/%d"),
        "export_end_date": current_month_end_date.strftime("%Y/%m/%d"),
        "export_start_date_json": get_date_json(prev_3_months_date),
        "export_end_date_json": get_date_json(current_month_end_date),
        "payroll_name": f"Morocco_Overtime_Export_{current_time}",
        "ma01_export_filename": f"{ma01_filename_prefix}_{current_time}",
        "ma02_ma03_export_filename": f"{ma02_ma03_filename_prefix}_{current_time}"
    }

def get_payroll_data_rows(item):
    return [
        item["Pay_Code_Code"],
        item["Employee_ID"].zfill(8),
        "M",
        "",
        item["Pay_Code_Hours"],
        datetime.strptime(item["Entry_Date"], "%Y/%m/%d").strftime("%Y-%m-%d"),
        ""
    ]

def get_empty_export_row():
    return [
        {
            "paycode": "",
            "employee_id": "",
            "format": "",
            "nbrbas": "",
            "hours": "",
            "entrydate": "",
            "entitlement": ""
        }
    ]

def get_location_uri(response, location):
    return list(map(lambda locations_data: locations_data["location"]["uri"],
        filter(lambda locations_data: locations_data["location"]["displayText"] == location
            and str(locations_data["hierarchyLevel"]) == "0", response)))[0]

def get_costcenter_uri(response, costcenter):
    return rail.find_first_by_attr_and_get_attr(response, "costCenter.displayText", costcenter, "costCenter.uri")

def filter_costcenter_hierarchy(response, config):
    costcenter_data=list(map(lambda costcenter_cells: {
            "costcenter": costcenter_cells["cells"][0]["textValue"],
            "uri": costcenter_cells["cells"][0]["uri"],
            "fullpath": [costcenter_data["textValue"]
                for costcenter_data in costcenter_cells["cells"][1]["cellCollection"]]
        }, response["rows"]))
    filtered_costcenters = list(filter(lambda costcenter_data:
        config.ma01_costcenter in costcenter_data["fullpath"] or
        config.ma02_costcenter in costcenter_data["fullpath"] or
        config.ma03_costcenter in costcenter_data["fullpath"], costcenter_data))
    return filtered_costcenters

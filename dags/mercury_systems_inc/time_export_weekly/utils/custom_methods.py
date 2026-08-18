from functools import lru_cache
import pendulum
import json
from datetime import datetime, timedelta
import rail

DEFAULT_PROJECT = "OVERHEAD"

def get_logging_details(config):
    """
    Generate logging details for time export.
    
    Args:
        config: Configuration object with time export settings
        
    Returns:
        dict: Dictionary containing export details like filenames and date ranges
    """
    today = pendulum.now(config.time_zone)
    if today.weekday != 0:
        today = today.start_of("week")
    current_time = today.strftime('%Y%m%d_%H%M%S')
    
    # Calculate date range for weekly export
    # For year deltas or changes
    start_date = today - timedelta(days=366)
    end_date = today- timedelta(days=3)
    
    return {
        "current_time": current_time,
        "time_export_filename": f"WeeklyTimeData_{current_time}",
        "time_export_filename_nodata": f"WeeklyTimeData_{current_time}_Nodata",
        "time_export_filename_cancelled": f"WeeklyTimeData_{current_time}_Cancelled",
        "export_start_date": start_date.strftime("%Y/%m/%d"),
        "export_end_date": end_date.strftime("%Y/%m/%d"),
        "export_start_date_json": {
            "year": start_date.year,
            "month": start_date.month,
            "day": start_date.day
        },
        "export_end_date_json": {
            "year": end_date.year,
            "month": end_date.month,
            "day": end_date.day
        },
        "time_zone": config.time_zone
    }


def get_export_datetime(response):
    """
    Extract creation datetime from export response.
    
    Args:
        response: Response from GetTimeDataExportDetails API
        
    Returns:
        str: Formatted datetime string
    """
    time_in_utc = response["creationDate"]["valueInUtc"]
    return pendulum.datetime(
        int(time_in_utc["year"]), int(time_in_utc["month"]), int(time_in_utc["day"]),
        int(time_in_utc["hour"]), int(time_in_utc["minute"]), int(time_in_utc["second"])
    ).strftime("%Y%m%d_%H%M%S")


@lru_cache(maxsize=128)
def get_batch_creation_datetime():
    """
    Get the batch creation datetime from the task context.
    
    Returns:
        str: Export creation datetime from task context
    """
    return rail.result("get_export_creation_datetime")


@lru_cache(maxsize=128)
def get_time_export_uri():
    """
    Get the time export URI from the task context.
    
    Returns:
        str: Export URI identifier
    """
    return (rail.result("time_data_export.get_export_uri")).split(":")[-1]

def get_time_data_csv_rows(item):
    """
    Format time data as CSV row for export.
    
    Args:
        item: Time entry data item
        index: Row index
        
    Returns:
        list: Formatted row data for CSV export
    """
    if not item:
        return []
    if item.get("timeoff_type_name"):
        item["project_name"] = item["project_code"] = DEFAULT_PROJECT
        item["task_name"] = item["task_code"] = item["timeoff_type_name"]
    return [
        item["employee_id"],	
        item["project_name"],
        item["project_code"],
        item["task_code"],
        item["task_name"],
        item["entry_date"],
        item["hours"],
        "Y" if item.get("employee_approval","Y") in ["Y"] else "N",
        "Y" if item.get("manager_approval") in ["Approved","Y"] else "N",
        item["employee_ou"],
        item["employee_charge_type"],
        item.get("user_first_name",""),
        item.get("user_last_name",""),
        item["employee_department"],
        item["charge_type"],
        item.get("time_entry_id",""),
        item.get("time_off_booking_id","")
    ]



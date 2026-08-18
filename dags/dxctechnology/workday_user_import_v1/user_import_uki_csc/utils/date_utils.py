"""
Common date utility functions for UK&I User Import
"""
from datetime import datetime, timedelta
import pendulum
from typing import Optional, Union
from dxctechnology.workday_user_import_v1.user_import_uki_csc.utils.constants import (
    DATE_FORMAT_YYYY_MM_DD, DATE_FORMAT_YYYY_DD_MM, 
    DATE_FORMAT_YYYYMMDD, DATE_FORMAT_YYYYMMDD_HHMMSS
)
INPUT_DATE_FORMAT = "%Y-%d-%m"

def parse_workday_date(date_string: Optional[str], default=None) -> Optional[pendulum.DateTime]:
    if not date_string:
        return default
    
    # Workday always sends dates as YYYY-DD-MM, we need to convert to YYYY-MM-DD
    # Split the date string
    parts = date_string.split('-')
    year, day, month = parts
    # Reconstruct in correct format YYYY-MM-DD
    corrected_date = f"{year}-{month}-{day}"
    return pendulum.parse(corrected_date)

def parse_date(date_string: Optional[str], default=None) -> Optional[pendulum.DateTime]:
    if not date_string:
        return default
    
    try:
        return pendulum.parse(date_string)
    except (ValueError, TypeError):
        return default

def format_workday_date_to_yyyy_mm_dd(date_string: Optional[str]) -> Optional[str]:
    parsed_date = parse_workday_date(date_string)
    return parsed_date.format(DATE_FORMAT_YYYY_MM_DD) if parsed_date else None

def format_date_yyyy_mm_dd(date_string: Optional[str]) -> Optional[str]:
    parsed_date = parse_date(date_string)
    return parsed_date.format(DATE_FORMAT_YYYY_MM_DD) if parsed_date else None

def format_date_yyyymmdd(date_string: Optional[str]) -> Optional[str]:
    parsed_date = parse_date(date_string)
    return parsed_date.format(DATE_FORMAT_YYYYMMDD) if parsed_date else None

def format_timestamp() -> str:
    return pendulum.now().format(DATE_FORMAT_YYYYMMDD_HHMMSS)

def get_today_yyyy_mm_dd() -> str:
    return pendulum.today().format(DATE_FORMAT_YYYY_MM_DD)

def get_week_start_date(date_string: str) -> str:
    date_obj = parse_date(date_string, pendulum.today())
    start_of_week = date_obj.start_of('week')
    return start_of_week.format(DATE_FORMAT_YYYY_MM_DD)

def compare_dates(date1: str, date2: str) -> int:
    parsed_date1 = parse_date(date1)
    parsed_date2 = parse_date(date2)
    
    if not parsed_date1 or not parsed_date2:
        return 0
    
    if parsed_date1 < parsed_date2:
        return -1
    elif parsed_date1 > parsed_date2:
        return 1
    return 0

def is_date_in_past(date_string: str) -> bool:
    parsed_date = parse_date(date_string)
    if not parsed_date:
        return False
    
    return parsed_date.date() < pendulum.today().date()

def is_date_in_future(date_string: str) -> bool:
    parsed_date = parse_date(date_string)
    if not parsed_date:
        return False
    
    return parsed_date.date() > pendulum.today().date()

def get_json_date_from_date_str(date_str, _format=None):
    if not date_str:
        return {}
    if _format:
        _date = datetime.strptime(date_str, _format)
    else:
        _date = datetime.strptime(date_str, INPUT_DATE_FORMAT)
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }

def get_timesheet_period_effective_date_for_instance(instance):
    if instance == "trial":
        return "2025-01-09"
    if instance == "prod":
        return "2026-01-04"
    return "2025-01-09"


def _get_effective_date_based_on_work_week(work_week: str, work_week_starts_with_check: list, return_as_dict:bool = False):
    today = datetime.now()
    current_weekday = today.weekday()  # Monday=0, Sunday=6
    
    
    # Handle None or empty work_week gracefully
    if not work_week:
        return {}
        
    work_week_parts = work_week.lower().split()
    if not work_week_parts:
        raise ValueError(f"Invalid work week format: {work_week}")
        
    work_week_start = work_week_parts[0]
    
    # Validate work_week_start is a valid weekday
    valid_weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    if work_week_start not in valid_weekdays:
        raise ValueError(f"Invalid work week start: {work_week_start}. Must be one of {valid_weekdays}")

    def days_back_to_target(target_weekday: int) -> int:
        if current_weekday == target_weekday:
            return 0
        return (current_weekday - target_weekday) % 7
    
    # Map day name to weekday number (Monday=0, Sunday=6)
    weekday_map = {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3, 
                    "friday": 4, "saturday": 5, "sunday": 6}
    
    # Determine target start day based on configuration
    if work_week_start == "saturday":
        if "saturday" in work_week_starts_with_check:
            target_day = weekday_map["saturday"]
        elif "sunday" in work_week_starts_with_check:
            target_day = weekday_map["sunday"]
        else:
            target_day = weekday_map["monday"]
    else:
        target_day = weekday_map.get(work_week_start, weekday_map["monday"])
    
    days_to_subtract = days_back_to_target(target_day)
    return_date = today - timedelta(days=days_to_subtract)
    if not return_as_dict:
        return return_date
    return {
        "day": return_date.day,
        "month": return_date.month,
        "year": return_date.year
    }


def build_json_formatted_dates(item: dict
                               , instance:str, work_week:str) -> dict:
    return {
        "hire_date": get_json_date_from_date_str(item.get("hiredate")),
        "service_date": get_json_date_from_date_str(item.get("servicedate")),
        "term_date": get_json_date_from_date_str(item.get("termdate")),
        "supervisor_date": get_json_date_from_date_str(item.get("supervisordate")),
        "location_effective_date": get_json_date_from_date_str(item.get("locationeffectivedate")),
        "cost_center_effective_date": get_json_date_from_date_str(item.get("costcentereffectivedate")),
        "exempt_effective_date": get_json_date_from_date_str(item.get("exempteffectivedate")),
        "work_shift_effective_date": get_json_date_from_date_str(item.get("workshifteffectivedate")),
        "job_change_effective_date": get_json_date_from_date_str(item.get("jobchangeeffectivedate")),
        "additional_data_effective_date": get_json_date_from_date_str(item.get("additionaldataeffectivedate")),
        "ia_start_date": get_json_date_from_date_str(item.get("iastartdate")),
        "ia_end_date": get_json_date_from_date_str(item.get("iaenddate")),
        "employee_representative_effective_date": get_json_date_from_date_str(item.get("employeerepresentativeeffectivedate")),
        "timesheet_period_effective_date": get_json_date_from_date_str(get_timesheet_period_effective_date_for_instance(instance)),
        "dob": get_json_date_from_date_str(item.get("dob")),
        "work_week": _get_effective_date_based_on_work_week(work_week, [], return_as_dict=True)
    }
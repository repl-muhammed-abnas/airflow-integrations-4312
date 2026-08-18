from pendulum import now
from datetime import datetime, timedelta
from functools import lru_cache

DATE_FORMAT="%m/%d/%Y"
SQL_DATE="%Y-%m-%d"

def get_csv_filename(time_zone):
    current_time = now(time_zone)
    filename = f"GLProjectTime_{current_time.strftime('%Y%m%d_%H%M%S')}.csv"
    return filename

def get_date_range():
    today = now(tz="America/New_York")

    if today.weekday() != 0:
        today = today.start_of("week")

    # Entry dates: Today-3 to Today-9 (Sat to Fri of previous week)
    entry_end_date = today - timedelta(days=3)  # Friday
    entry_start_date = today - timedelta(days=9)  # Saturday
    beyond_nine_days = today - timedelta(days=10)
    # Approval dates: Today-1 to Today-7 (Mon to Sun of previous week)
    approval_end_date = today - timedelta(days=1)  # Sunday
    approval_start_date = today - timedelta(days=7)  # Monday

    return {
        "start_date": entry_start_date.strftime("%m/%d/%Y"),
        "end_date": entry_end_date.strftime("%m/%d/%Y"),
        "approval_start_date": approval_start_date.strftime("%m/%d/%Y"),
        "approval_end_date": approval_end_date.strftime("%m/%d/%Y"),
        "beyond_nine_days": beyond_nine_days.strftime("%m/%d/%Y"),
    }

@lru_cache(maxsize=128)
def get_timesheet_end_date(date):
    if not date:
        return ""
    return  datetime.strftime(datetime.strptime(date, "%b %d, %Y"),"%m/%d/%Y")

def format_csv_row(item):
    """
    Format report data as CSV row.
    
    Args:
        item: Row data from report
        index: Row index (optional)
        
    Returns:
        list: Formatted row data
    """
    if not item:
        return []
    timesheet_end_date = get_timesheet_end_date(item.get('week_ending', ''))
    return [
        item.get('full_name', ''),
        item.get('employee_id', ''),
        item.get('uses_activity', ''),
        item.get('union_code', ''),
        item.get('job_code', ''),
        item.get('pay_type', ''),
        timesheet_end_date,
        item.get('bu', ''),
        item.get('department', ''),
        item.get('activity_pay_Code', ''),
        item.get('weekly_hours',""),
        item.get('weekly_earnings', ''),
    ]

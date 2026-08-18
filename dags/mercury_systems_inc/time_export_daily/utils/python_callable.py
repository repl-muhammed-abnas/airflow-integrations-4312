import pendulum
from datetime import datetime, timedelta
from functools import lru_cache
import rail

DATE_FORMAT="%Y-%m-%d"

def get_csv_filename(time_zone):
    """
    Generate filename for daily time export CSV.
    
    Args:
        company_key: Company identifier
        time_zone: Timezone for timestamp
        
    Returns:
        str: Formatted filename with timestamp
    """
    current_time = pendulum.now(time_zone)
    filename = f"Daily_Export_{current_time.strftime('%Y%m%d_%H%M%S')}.csv"
    return filename

def get_logging_details(config):
    """
    Generate date filters for the report.
    
    Returns:
        dict: Date filters for entry date (last 10 days) and approval date (today or yesterday)
    """
    today = pendulum.now(tz=config.time_zone)
    nine_days_ago = (today - timedelta(days=9)).strftime(DATE_FORMAT)
    today=today.strftime(DATE_FORMAT)
    return {
        "entry_end_date": today,
        "entry_start_date": nine_days_ago
    }

@lru_cache(maxsize=128)
def get_entry_date(date):
    return  datetime.strftime(datetime.strptime(date, "%Y-%m-%d"),"%m/%d/%Y")

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

    entry_date = get_entry_date(item.get("posting_date", ''))

    return [
        item.get("employee_id", ''),
        item.get("project_name", ''),
        item.get("project_id", ''),
        item.get("task_id", ''),
        item.get("task_name", ''),
        entry_date,
        item.get("hours", ''),
        item.get("employee_approval", ''),
        item.get("manager_approval", ''),
        item.get("employee_ou", ''),
        item.get("employee_charge_type", ''),
        item.get("first_name", ''),
        item.get("last_name", ''),
        item.get("employee_department", ''),
        item.get("charge_type", ''),
        item.get("time_entry_id", ''),
        ""
    ]

def get_report_param():
    return {
    "reportParameters": [
        {
            "reportUri": rail.result('get_report_details')["uri"],
            "filterValues": [
                {
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
                    'displayText', "ProgramFilter", 'uri'),
                    "value": rail.result("get_wo_program_uri")
                }
            ],
            "outputFormatUri": "urn:replicon:report-output-format-option:csv"
        }
    ]
}
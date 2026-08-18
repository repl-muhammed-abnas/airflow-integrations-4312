import pendulum
import re
from dateutil import parser as date_parser


# Pre-compile regex for better performance
TIME_PATTERN = re.compile(r'\s+\d{1,2}:\d{2}:\d{2}.*$')
EMPTY_VALUES = ('', 'nan', None)


def format_date(date_str):
    """Format date string to DD-MM-YYYY, handles ranges"""
    if not date_str or str(date_str).strip() in EMPTY_VALUES:
        return date_str
    
    date_str = str(date_str).strip()
    
    if ' - ' in date_str:
        parts = date_str.split(' - ', 1)
        return f"{date_parser.parse(TIME_PATTERN.sub('', parts[0].strip())).strftime('%d-%m-%Y')} - {date_parser.parse(TIME_PATTERN.sub('', parts[1].strip())).strftime('%d-%m-%Y')}"
    
    return date_parser.parse(TIME_PATTERN.sub('', date_str)).strftime('%d-%m-%Y')


def transform_report_row(row, date_columns):
    """Transform row dates to DD-MM-YYYY format"""
    if not row:
        return row
    
    date_cols = set(date_columns)
    for col, value in row.items():
        if col in date_cols and value:
            row[col] = format_date(value)
    
    return row


def get_csv_filename(company_key, time_zone, report_name, report_type):
    current_time = pendulum.now(time_zone)
    code = report_name.split('-')[-1].strip().replace(" ", "_")
    filename = f"{company_key}_{report_type}_{code}_{current_time.strftime('%d%m%Y_%H%M%S')}.csv"
    return filename

def get_file_name(filename, time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "filename": f"{filename}.csv",
        "archive_filename": f"{filename}_{current_time.strftime('%Y%m%d')}.csv"
    }


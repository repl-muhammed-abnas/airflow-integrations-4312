# Instance
environment = "pre-production"
region = 'us-east-1'

# Max active runs
max_active_run_master = 1
max_active_runs_child = 5

# Parallel count
parallel_count = 5

# Timeout
execution_timeout_days = 14

# Timezone
usa_timezone = "America/New_York"
india_timezone = "Asia/Kolkata"
emec_timezone = "America/New_York"

# Schedule Interval
usa_schedule_interval = "0 1 * * *"  # Daily at 1 AM EST
india_schedule_interval = "0 1 * * *"  # Daily at 1 AM IST
emec_schedule_interval = "0 3 * * *"  # Daily at 3 AM EST

# Replicon Report Name
REPLICON_UNAPPROVED_TIME_REPORT_NAME = "***Time extract to Datalake - Unapproved time***"

### Company Code set
# USA Technology and loan services company codes
USA_COMPANY_CODE_SET_1 = {
    "codes": {"US132", "US190", "US117"},
    "file_name_prefix": "US132US190US117",
}

# USA Advisory company codes
USA_COMPANY_CODE_SET_2 = {
    "codes": {"US101"},
    "file_name_prefix": "US101",
}

# USA Managed Services
USA_COMPANY_CODE_SET_3 = {
    "codes": {"US146"},
    "file_name_prefix": "US146",
}

IND_COMPANY_CODE_SET_1 = {
    "codes": {"IN553"},
    "file_name_prefix": "IN553",
}

EMEC_COMPANY_CODE_SET_1 = {
    "codes": {"AE662", "AE663", "BE398", "CA270", "DE396", "EI386",
              "FR480", "GB320", "KR448", "LI240", "SW664", "NL397"},
    "file_name_prefix": "EMEC",
}

# Report file header
REPORT_COLUMN_HEADER_MAP = {
    "Employee ID": "employee_id",
    "Timesheet Start Date": "timesheet_start_date",
    "User Name": "user_name",
    "Login Name": "login_name",
    "Entry Date": "entry_date",
    "Project Code": "project_code",
    "Task Code": "task_code",
    "Task Name": "task_name",
    "Work Location Code": "work_location_code",
    "Hours": "hours",
    "Company Code (Current)": "company_code_current",
    "Timesheet Approval Status": "timesheet_approval_status",
    "Time Off Type": "time_off_type",
    "Time Off Hours": "time_off_hours",
    "Timesheet Period": "timesheet_period",
    "Financial System": "financial_system",
    "FMLA": "fmla",
    "Short Entry ID": "short_entry_id",
    "Location (Current) (Full Path)": "location_current_full_path",
    "Task URI": "task_uri",
    "Source System": "source_system",
    "Task Name (Full Path)": "task_name_full_path",
    "Comments": "comments",
}

# Export file layout
EXPORT_FILE_LAYOUT = [
    "unique_id",
    "employee_id",
    "user_name",
    "entry_date",
    "project_code",
    "task_code",
    "work_location_code",
    "hours",
    "company_code",
    "timesheet_approval_status",
    "time_off_type",
    "time_off_hours",
    "plc_name",
    "plc",
    "timesheet_period",
    "financial_system",
    "short_entry_id",
    "comments",
]

# Export file columns
EXPORT_FILE_HEADER = [
    "Unique ID",
    "Employee ID",
    "User Name",
    "Entry Date",
    "Project Code",
    "Task Code",
    "Work Location Code",
    "Hours",
    "Company Code",
    "Timesheet Approval Status",
    "Time Off Type",
    "Time Off Hours",
    "PLC Name",
    "PLC",
    "Timesheet Period",
    "Financial System",
    "Short Entry ID",
    "Comments",
]

# ***Time extract to Datalake - Unapproved time*** Column Order
EXPECTED_REPORT_COLUMNS = "Employee ID,Timesheet Start Date,User Name,Login Name,Entry Date,Project Code,Task Code,Task Name,Work Location Code,Hours,Company Code (Current),Timesheet Approval Status,Time Off Type,Time Off Hours,Timesheet Period,Financial System,FMLA,Short Entry ID,Location (Current) (Full Path),Task URI,Source System,Task Name (Full Path),Comments"

# File name format
FILE_NAME_FORMAT = "DL_Unapproved_{week_type}_{file_name_prefix}_{instance}_{timestamp}.csv.pgp"
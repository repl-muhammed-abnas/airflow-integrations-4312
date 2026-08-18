"""
Configuration file for Unisys Fieldglass Time Export Integration
Contains only the configurations that are actually used in the integration

Based on design document: Replicon to Fieldglass Integration - Technical Specification V1.1
"""
region = "us-east-1"
environment = "pre-production"
# DAG execution configuration
dag_max_active_run_master = 1
dag_max_active_run_child = 4
max_active_runs_export_generation = 1
gather_entries_logs_timeout_hours = 2
execution_timeout_days = 14

# Report configuration based on design doc
# Design states: "report based on the template Employee Pay Details and Timesheet Period"
employee_pay_report_name = 'Employee Pay Details - Fieldglass'  # Primary report for detailed timesheet data
timesheet_period_report_name = 'Timesheet Period Report - Fieldglass'  # Secondary report for period validation and zero hours detection

# Time zone configuration
utc_timezone = 'UTC'

# Master-Child DAG configuration
trigger_parallel_dagrun_count_process_ts_entries = 2

# CSV header as per design doc field mapping
csv_header = [
    'WorkOrder_ID',
    'Date',
    'Rate_Category_Code',
    'Sat_Hrs',
    'Sun_Hrs',
    'Mon_Hrs',
    'Tue_Hrs',
    'Wed_Hrs',
    'Thu_Hrs',
    'Fri_Hrs'
]

# Expected report columns for validation
expected_employee_pay_report_columns = 'Timesheet Period,Timesheet Period Uri,Pay Code Hours,Approval Date/Time,Approval Status,User Name,Entry Date,Pay Code Name,Purchase Order ID,User Type (Current) (Full Path),Week (Timesheet Start Date),Timesheet Start Date'
expected_timesheet_period_report_columns = 'Timesheet Period,Timesheet Period Uri,Total Hrs (In Period),Approval Date/Time,Approval Status,User Name,Pay Code Name,Purchase Order ID (Current),User Type (Current) (Full Path),Week (Timesheet Start Date),Timesheet Start Date'
# Column mapping for Employee Pay report
employee_pay_report_columns = {
    "Timesheet Period": "timesheet_period",
    "Timesheet Period Uri": "timesheet_period_uri",
    "Pay Code Hours": "total_hours",
    "Approval Date/Time": "approval_date",
    "Approval Status": "approval_status",
    "User Name": "user_name",
    "Entry Date": "entry_date",
    "Pay Code Name": "pay_code_name",
    "Purchase Order ID": "purchase_order_id",
    "User Type (Current) (Full Path)": "user_type_full_path",
    "Week (Timesheet Start Date)": "week_start_date",
    "Timesheet Start Date": "timesheet_start_date"
}

# Column mapping for Timesheet Period report
timesheet_period_report_columns = {
    "Timesheet Period": "timesheet_period",
    "Timesheet Period Uri": "timesheet_period_uri",
    "Total Hrs (In Period)": "total_hrs",
    "Approval Date/Time": "approval_date",
    "Approval Status": "approval_status",
    "User Name": "user_name",
    "Pay Code Name": "pay_code_name",
    "Purchase Order ID (Current)": "purchase_order_id",
    "User Type (Current) (Full Path)": "user_type_full_path",
    "Week (Timesheet Start Date)": "week_start_date",
    "Timesheet Start Date": "timesheet_start_date"
}
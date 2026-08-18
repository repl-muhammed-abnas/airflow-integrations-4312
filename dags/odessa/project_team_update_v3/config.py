schedule_interval = "0 */1 * * *"

region = 'us-east-1'
environment = "pre-production"
pacific_timezone = 'America/Los_Angeles'

execution_timeout_days = 14
master_dag_max_active_runs = 1
child_dag_max_active_runs = 14
billing_rate_child_max_active_runs = 14

sftp_sensor_timeout_minutes = 10

userdata_report_name = "Userdata"

billing_rate_suffix_by_location = {
    "INDIA": "OFS",
    "USA": "ONS",
}

default_billing_rate_uri = "urn:replicon:project-specific-billing-rate"
report_output_format_uri = "urn:replicon:report-output-format-option:csv"

input_csv_columns = {
    "employeeid": "employeeid",
    "loginname": "loginname",
    "projectname": "projectname",
    "action": "action",
    "role": "role",
}

userdata_report_columns = {
    "User Email": "useremail",
    "Login Name": "loginname",
    "Customer Role (Current)": "customerrole",
    "useruri": "useruri",
    "Holiday Calendar": "holidaycalendar",
    "HolidayCalendarUri": "holidaycalendaruri",
    "Location (Current)": "location",
}



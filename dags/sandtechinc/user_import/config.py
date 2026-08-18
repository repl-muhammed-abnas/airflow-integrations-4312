# Sand Tech Inc - User Import Configuration
# Static configuration shared across all instances

region = 'us-east-1'
time_zone = "UTC"
environment = "pre-production"

# Processing Configuration
execution_timeout_days = 14
child_dag_max_active_runs = 2
master_dag_interval = 30

# Date format for parsing input file
date_format = '%d/%m/%Y'

# Static Assignments
default_timezone = "(UTC+2:00) South Africa Standard Time"
default_work_week = "urn:replicon:day-of-week:monday"
default_office_schedule = "8 hours/day; Mon-Fri"
default_timesheet_template = "Standard Timesheet"
default_timesheet_approval_path = "Default"
default_timesheet_period = "Weekly starting on a Monday"
default_timeoff_template = "Time Off"
default_timeoff_approval_path = "Default"
default_employee_type = "Full time"
default_notification_when_to_send = "urn:replicon:notification-delivery-option:any-time"

# Permissions
project_resource_permission = "Project Resource"
supervisor_permission = "Supervisor"

# Reference file name
reference_file_name = "sandtechinc_reference.csv"
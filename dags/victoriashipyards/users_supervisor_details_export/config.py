region = 'us-east-1'
environment = 'pre-production'

company_key = 'seaspanvslsb'

max_active_runs = 1

user_details_report = '***Users Supervisor Details Report***'
expected_report_columns = "User Name,Employee ID,UserUri,User Supervisor Name (Current),User Supervisor Email address,SupervisorUri,Employee Type (Current)"
export_headers = ["Username", "Employee ID", "User Supervisor Name", "User Supervisor Email Address", "User Supervisor Employee ID"]
reference_headers = ["username", "employeeid", "supervisorname", "supervisoremail", "supervisoremployeeid", "sha256"]

time_zone = 'PST8PDT'

run_report_wait_timeout = 60 * 60 * 24
log_file_link_expiry = 7*24*60*60
execution_timeout_days = 14
execution_timeout_mins_write_csv = 90
thread_pool_size_write_csv = 10

schedule_interval = "0 23 * * *"

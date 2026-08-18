region = 'us-east-1'
environment = 'pre-production'
time_zone = 'Etc/GMT+8'

master_dag_interval = 1
max_active_runs = 1
update_timeofftype_dag_max_active_runs = 3
report_name = "**Rehire Logic inititation Report**"
expected_report_columns = 'User Name,Login Name,User Start Date,User Email,Date for rehire calculation,user uri,Full/Part Time,Regular/Temporary,Adjusted Service Date'
execution_timeout_days = 14

use_reference_file = "No"
disable_threshold = 200
schedule_interval='0 1 * * *'
date_format = '%B %d, %Y'







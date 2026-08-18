region = 'eu-central-1'
environment = "pre-production"
time_zone = "America/New_York"
master_schedule_interval = 30
execution_timeout_days = 14

max_active_runs_master = 1
max_active_runs_child_1 = 2
max_active_runs_child_2 = 3

schedule_interval = "0 1 1 * *"

report_name = "** User base report - Shift Automation - MONTHLY"

batch_size = 50
column_name = "User Name,Employee ID,User Uri,User Status,Country,Schedule,Legal Entity Code,Acquired Company"

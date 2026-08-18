region = 'eu-central-1'
environment = "pre-production"
time_zone = "America/New_York"
master_schedule_interval = 30
execution_timeout_days = 14

max_active_runs_master = 1
max_active_runs_child = 5

schedule_interval = "0 1 * * *"

report_name = "** User base report - Shift Automation1"

batch_size = 1

column_name = "User Name,Employee ID,User Uri,User Status,User Start Date,Country,Schedule,Onsite Direct Recruit,Onsite Start Date,Legal Entity Code,Acquired Company,FJEmpIdentifier"

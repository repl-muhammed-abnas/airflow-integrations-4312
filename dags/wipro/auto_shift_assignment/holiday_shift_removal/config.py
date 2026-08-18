region = 'eu-central-1'
environment = "pre-production"
time_zone = "America/New_York"
master_schedule_interval = 30
execution_timeout_days = 14

max_active_runs_master = 1
max_active_runs_child_1 = 2
max_active_runs_child_2 = 3

schedule_interval = "0 12 * * *"

report_name = "**User Shift Removal For Holidays"

batch_size = 1
column_name ="Employee ID,User Name,Holiday Calendar,UserUri,Holiday Calendar Uri,Country,Schedule,Acquired Company,Legal Entity Code,User Start Date,Onsite Direct Recruit,Onsite Start Date"

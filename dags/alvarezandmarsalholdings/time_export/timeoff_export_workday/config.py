region = 'us-east-1'
environment = 'pre-production'

time_zone = "US/Pacific"

execution_timeout_days = 14
http_post_timeout_hours = 4

max_active_runs_child = 1

timeoff_export_file_format = 'A&M Workday timeoff export'

timeoff_export_workday_file_name_format = "RN_TimeOff_"

# Single Element in tuple is not considered as tuple
time_off_types_to_exclude_from_export = ("Holiday", "Holiday")

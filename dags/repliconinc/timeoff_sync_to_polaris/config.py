region = 'us-east-1'
environment = "pre-production"
# environment = "trial_polaris"
max_active_runs = 1
max_active_runs_child = 5
schedule_interval = "0,30 * * * *"
last_run_var_name = "repliconinc_timeoff_sync_to_polaris_last_run_time"
timezone = 'America/Los_Angeles'
timezone_utc = 'UTC'

execution_timeout_days=7
parallel_count=5

report_name_repliconinc="Timeoff booking sync to Polaris"
report_name_polaris="Enabled user list for integration"

column_order_replicon_report = "User Name,User Email,Login Name,Time Off Date,Time Off Hrs,Units,Time Off Type,Booking Start Date/Time,Time Off Comments,Approval Status,Approval Date,Time Off Days,TimeOffBookingUri,Employee ID,Modified On"
column_order_polaris_report="User Name,User Email,Login Name,UserUri,Employee ID"
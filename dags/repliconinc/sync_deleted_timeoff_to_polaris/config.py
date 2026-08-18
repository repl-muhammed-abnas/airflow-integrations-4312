region = 'us-east-1'
environment = "pre-production"

max_active_runs = 1
max_active_runs_child = 5
schedule_interval = "0 23 * * *"
timezone='UTC'

execution_timeout_days=7
parallel_count=5

report_name_repliconinc="Deleted Timeoff booking sync to Polaris"
report_name_polaris="Enabled user list for integration"

column_order_replicon_report = "User Name,Login Name,Current Start Date,Action,Modified By,Modified On,Field,Original Value,New Value,Department (Current),departmentcheck,TimeOffId,Employee ID"
column_order_polaris_report="User Name,User Email,Login Name,UserUri,Employee ID"
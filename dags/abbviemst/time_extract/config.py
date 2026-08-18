environment = 'pre-production'
region = 'us-east-1'

# Schedule: Cron runs at 8 AM on 2nd, 3rd, and 4th of each month
# Conditional check ensures execution only on the correct day based on Workato logic:
# - If 1st is Sunday → run on 3rd
# - If 1st is Monday-Thursday → run on 2nd
# - If 1st is Friday-Saturday → run on 4th
master_schedule_interval = "0 8 2-4 * *"

execution_timeout_days = 14

max_active_runs_master = 1
max_active_runs_child = 1

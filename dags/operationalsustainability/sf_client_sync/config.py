region = 'us-east-1'
environment = 'pre-production'

master_schedule_interval = 5
execution_timeout_days = 14

max_active_runs_master = 1
max_active_runs_child = 10
trigger_parallel_dagrun_count = 5

time_zone = 'UTC'

account_types_to_sync = "Client, Active"
#Enter a comma separated list of account types to be synced. Put ALL if all account types should be synced.

sync_accounts_with_no_types = False
#True if Salesforce accounts with no account type specified should be synced as well. False otherwise.

to_update = False
#True if updates to Salesforce accounts should be synced as well

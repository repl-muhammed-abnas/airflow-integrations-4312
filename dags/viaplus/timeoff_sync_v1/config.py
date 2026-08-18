region = 'us-east-1'
environment = "pre-production"

sumo_conn_id = 'sumologic-dagrunlogger'

time_zone = "Asia/Kolkata"
# Hourly at 30 mins past the hour
schedule_interval = "30 * * * *"

max_active_runs = 1
max_active_booking_child = 5
execution_timeout_days = 14

# Airflow variable names
lookback_hours_var_name = "viaplus_timeoff_sync_lookback_hours"
can_run_batch_task_booking_child_var_name = "viaplus_timeoff_sync_booking_child_can_run_batch_task"
can_run_batch_task_master_var_name = "viaplus_timeoff_sync_master_can_run_batch_task"

# Default lookback hours for change detection
default_lookback_hours = 1

# Legal entity filter (Cost Center group in Replicon)
legal_entity_filter = "VPTI Solutions Private Limited"

# Keka API configuration
keka_auth_url = "https://login.keka.com/connect/token"
keka_api_scope = "kekaapi"
keka_page_size = 1000

# Keka API Configuration
KEKA_GRANT_TYPE = "kekaapi"
KEKA_SCOPE = "kekaapi"
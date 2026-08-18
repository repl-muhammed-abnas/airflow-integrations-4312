region = 'us-east-1'
environment = "pre-production"

master_max_active_run = 1
max_active_runs_child = 15
execution_timeout_days = 14
parallel_count = 10
parallel_count_clients = 3
parallel_count_cost_centers = 3

PROJECT_BATCH_COUNT = 10
RESOURCE_ASSIGNMENT_BATCH_SIZE = 500

file_sensor_timeout = 10
master_dag_interval = 30
time_zone = "US/Eastern"

DATE_FORMAT_INPUT = "%Y-%m-%d"
MAX_FIELD_LENGTH = 255

log_file_download_link_expiry_in_sec= 7 * 24 * 60 * 60

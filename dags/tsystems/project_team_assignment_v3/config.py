region = 'eu-central-1'
environment = "pre-production"

time_zone = "Etc/UTC"

execution_timeout_days = 14

master_max_active_runs = 1
child_max_active_runs = 1
allocation_child_max_active_runs = 5
max_active_runs_process_log_generation = 1

# Number of child-log artifacts sent to each log-generation child run.
log_generation_batch_size = 1000

# Batch execution configuration
ALLOCATION_BATCH_COUNT = 20
parallel_count = 20

# DAG scheduling
schedule_interval = "0/30 * * * *"

DATETIMEFORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

thread_pool_size_write_csv = 10
execution_timeout_write_csv = 2

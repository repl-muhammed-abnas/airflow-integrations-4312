region = 'us-east-1'
environment = 'pre-production'

# DAG execution settings
execution_timeout_days = 7
child_dag_max_active_runs = 5
max_active_runs = 1

# Employee sync specific settings
employee_sync_interval_minutes = 10
country_code = 'US'
initial_sync_time = '1970-01-01T00:00:00.000Z'

# Adaptive chunking: below the threshold each employee gets its own child DAG run
# (preserves per-employee granularity); above it, employees are grouped into chunks
# of employee_sync_chunk_size to collapse PATCH /users/sync calls.
employee_sync_low_volume_threshold = 10
employee_sync_chunk_size = 1000

# Common settings
ce_time_format = '%Y-%m-%dT%H:%M:%S.%fZ'
internal_email = ['procoreintegrationsupport@deltek.com']
is_paused_upon_creation = True

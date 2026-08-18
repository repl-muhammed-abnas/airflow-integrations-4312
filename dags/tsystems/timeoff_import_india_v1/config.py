region = "eu-central-1"
environment = 'pre-production'

# DAG execution settings
max_active_runs_master = 1
max_active_runs_child = 5
execution_timeout_days = 14

gather_timeoff_logs_timeout_hours = 2

# Scheduling
file_sensor_timeout = 10
master_dag_interval = 30

# Timezone settings
timezone = 'Etc/UTC'

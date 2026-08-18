
region = "us-east-1"
environment = "pre-production"

file_sensor_timeout = 10
max_active_runs = 1
child_dag_max_active_runs = 10
execution_timeout_days = 14
master_schedule_interval = 30

# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'

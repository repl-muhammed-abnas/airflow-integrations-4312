region = 'eu-central-1'
environment = 'pre-production'

europe_timezone = 'Europe/Paris'

max_active_runs_master = 1
child_dag_max_active_runs = 1

schedule_interval = '59 23 15 7 *'

batch_size = 4  # This is not a true dynamic value, for changing this, further code changes are required
filter_name = "EntryDateFilter"

execution_timeout_days = 14
thread_pool_size_write_csv = 10
execution_timeout_write_csv = 6

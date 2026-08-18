region = 'us-east-1'
environment = 'pre-production'

master_dag_interval = 30
master_dag_max_active_runs = 1
child_dag_max_active_runs = 20
max_active_runs_split_csv_batch = 1

dag_max_active_tasks = 128
execution_timeout_days = 14
execution_timeout_hours = 4

file_merge_count = "Tiger_Assignee_file_merge_count"

utc_timezone = 'Etc/UTC'
schedule_interval = '0 */3 * * *'

BATCH_SIZE = 10000

region = 'eu-central-1'
environment = 'pre-production'
max_active_runs = 5
execution_timeout_days = 14
child_dag_max_active_runs = 1
report_name = "Distance Traveled Report - NLD (Automation)"
schedule_interval='59 23 15 7 *'
batch_size = 2
europe_timezone ='Europe/Paris'
filter_name = "EntryDateFilter"

thread_pool_size_write_csv = 10
execution_timeout_write_csv = 2

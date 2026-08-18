environment = 'pre-production'
region = 'us-east-1'

master_max_active_run = 1
execution_timeout_days = 14
execution_timeout_days_for_posting = 1
http_post_timeout_hours = 4

time_zone = "Etc/UTC"
daily_run_schedule_interval = "*/30 * * * *"

time_export_file_format = "A&M S4HC Time Export"
sumo_conn_id = 'sumologic-exportlogger'

post_to_endpoint_max_active_run = 3

PROJECT_PROFILE_VALUE = ['P001','YP04','YP03']

report_name = "Project-Task Hierarchy"

child_dag_batch_size = 500

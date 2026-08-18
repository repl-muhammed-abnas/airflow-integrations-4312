region = 'us-east-2'
environment = 'pre-production'

master_dag_max_active_runs = 1
child_dag_max_active_runs = 10


dag_max_active_tasks = 128
execution_timeout_days = 14

file_merge_count = "DXC_GSAP_Billing_Key_file_merge_count"

utc_timezone = 'Etc/UTC'
schedule_interval = '30 */1 * * *'

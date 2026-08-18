region = 'us-east-1'
environment = 'pre-production'

max_active_runs = 10
execution_timeout_days = 1

conflict_search_report_path = '/Custom/Project/0000060666_ConflictSearchReport'
process_queue_id = 'REPORTS'

poll_interval_seconds = 120
poll_max_attempts = 25

# Ops-team alert recipients (templated — resolved per-instance from Airflow Variables)
alert_email = '{{ var.value.dagrun_failure_alert_email }}, Vantagepoint_Alerts@exponent.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'


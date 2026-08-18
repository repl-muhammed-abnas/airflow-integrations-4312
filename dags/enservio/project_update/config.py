region = "us-east-1"
environment = "pre-production"

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
master_dag_interval = 30
master_max_active_run = 1
execution_timeout_days = 14

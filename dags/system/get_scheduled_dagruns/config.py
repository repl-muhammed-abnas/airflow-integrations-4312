region = 'all'
environment = 'all'

# This is required for WriteCSVFileOperator
replicon_conn_id = 'airflow-replicon-admin'

alert_email = 'MPTeamReplicon@deltek.com, {{ var.value.dagrun_failure_alert_email }}'

execution_timeout_minutes = 15

max_days_difference = 3

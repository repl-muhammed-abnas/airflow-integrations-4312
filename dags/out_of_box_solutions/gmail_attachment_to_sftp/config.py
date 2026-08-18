region = 'us-east-1'
environment = ['pre-production','production']

replicon_conn_id = 'airflow-replicon-admin'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

max_active_runs_master = 1
max_active_runs_child = 5
schedule_interval = "*/15 * * * *"
gmail_attachment_to_sftp_account_details = "gmail_attachment_to_sftp_account_details"

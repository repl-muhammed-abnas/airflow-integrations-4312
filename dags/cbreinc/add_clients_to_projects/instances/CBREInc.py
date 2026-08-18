region = 'us-east-1'
environment = 'production'

instance = "CBREInc"

company_key = 'CBREInc'
replicon_conn_id = 'cbreinc_replicon_Chris.Blade@cbre.com'

can_run_batch_task_var_name = f'cbreinc_add_clients_to_projects_can_run_batch_task_{instance}'
aws_conn_id = 'replicon.workato_S3_account'
s3_bucket = 'replicon-integrations-uswest'
client_reference_s3_file_path = f"{instance}/add_clients_to_projects/client_reference.csv"
execution_timeout_days = 14
child_dag_max_active_runs = 20

# "time_unit": "days",
# "trigger_every": "1",
# "trigger_at": "22:00:00",
# "timezone": "America/Chicago"
schedule_time_zone = 'PST'
schedule_interval = '0 22 * * *'

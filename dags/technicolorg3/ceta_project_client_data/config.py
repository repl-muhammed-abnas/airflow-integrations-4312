region = 'us-east-2'
environment = 'pre-production'
instance = 'trial'

company_key = 'technicolortrial'
replicon_conn_id = 'replicon-technicolortrial'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

bearer_token_var = 'technicolor_webhook_ceta_project_client_secret'

master_dag_interval = 30
execution_timeout_days = 14
master_dag_max_active_runs = 1
child_dag_max_active_runs = 5

sumo_conn_id = 'sumologic-exportlogger'
client_project_logs = 'Technicolor_CETA_Client_Project_Logs'
project_tasks_mapper = 'technicolor_project_tasks_mapper'

# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'
schedule_interval = '0 */3 * * *'

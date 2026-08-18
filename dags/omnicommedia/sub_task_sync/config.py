region = 'us-east-1'
environment = 'pre-production'
instance = 'trial'

company_key = 'OmnicomMediaafmig'
replicon_conn_id = 'replicon-OmnicomMediaafmig-automation'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'


master_dag_interval = 30
execution_timeout_days = 14
master_dag_max_active_runs = 1
child_dag_max_active_runs = 2

sumo_conn_id = 'sumologic-exportlogger'

# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'

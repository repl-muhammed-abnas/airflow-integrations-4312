region = 'eu-central-1'
environment = 'pre-production'
instance = 'trial'

company_key = 'omdsingaporepteltdafmig'
replicon_conn_id = 'replicon-omdsingaporepteltdafmig-admin'
sftp_conn_id = 'Airflow_migration_SFTP_eucentral'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

file_sensor_timeout = 10
master_dag_interval = 30
execution_timeout_days = 14
master_dag_max_active_runs = 1
child_dag_max_active_runs = 1
master_schedule_interval = 30
time_zone = 'UTC'
sumo_conn_id = 'sumologic-exportlogger'

# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'

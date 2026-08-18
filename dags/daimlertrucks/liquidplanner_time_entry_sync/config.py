instance = "trial"
region = "us-east-1"
environment = "pre-production"

company_key = "DaimlerTrucksafmig"

sftp_conn_id = "Airflow_migration_SFTP_useast2"
replicon_conn_id = "daimlertrucks-replicon-replicon"

input_filepath = "/liquidplanner"
archive_filepath = "/liquidplanner/archive"

archive_logs_filepath = "/liquidplanner/archive/logs"
successfull_records_filepath = "/liquidplanner/successfullrecords"
rejected_records_filepath = "/liquidplanner/rejectedrecords"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

file_sensor_timeout = 10
max_active_runs = 1
user_child_dag_max_active_runs = 5
child_dag_max_active_runs = 2
execution_timeout_days = 14
master_schedule_interval = 30

# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'

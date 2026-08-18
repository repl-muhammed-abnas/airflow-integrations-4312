region = 'us-east-1'
environment = 'pre-production'
company_key = 'galaxyusopcoinctrial01'
replicon_conn_id = 'galaxyusopcoinctrial01_replicon_admin'
sftp_conn_id = "repliconsftp"

master_dag_interval = 30
file_sensor_timeout = 10

execution_timeout_days = 14

child_dag_process_user_schedule_runs = 10

pgp_conn_id = "pgp_vialto_partners"

input_filepath = "/rit_test/user_schedule/input"
archive_filepath = "/rit_test/user_schedule/archive"
log_filepath = "/rit_test/user_schedule/logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

base_schedule_report_name = "*** User Schedule Import Base Report"
expected_report_columns = "Employee ID,Schedule Name (Current),useruri"

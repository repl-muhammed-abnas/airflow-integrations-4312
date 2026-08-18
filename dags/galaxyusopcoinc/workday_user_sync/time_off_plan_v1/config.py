region = 'us-east-1'
environment = 'pre-production'
company_key = 'galaxyusopcoinctrial01'
replicon_conn_id = 'galaxyusopcoinctrial01_replicon_admin'
sftp_conn_id = "repliconsftp"

master_dag_interval = 30
file_sensor_timeout = 10

execution_timeout_days = 14

mapper_name = "vialtop_timeoff_mapper"

extract_report_name = "**Users By Country TO Integration"
report_filter_name = "OEFilter_UserOEF33f431fce64e4404a93f8ab2df08c461"

pgp_conn_id = "pgp_vialto_partners"
input_filepath = "/rit_test/timeoffplan/input"
archive_filepath = "/rit_test/timeoffplan/archive"
log_filepath = "/rit_test/timeoffplan/logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'


TIMEOFF_DISABLE_CHECK_LIST = ["zdnu"]

master_max_active_runs = 1
create_timeoff_type_max_active_run = 5
disable_timeoff_type_max_active_run = 1
enable_timeoff_type_max_active_run = 1
assign_newly_created_timeoff_type_max_active_run = 5

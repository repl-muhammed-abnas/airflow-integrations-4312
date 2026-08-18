region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-RepliconIntGSAP'
sftp_conn_id = 'sftp_useast2'

input_filepath_attr1 = '/rit_test/gsab_billing_key/processing'
archive_filepath = '/rit_test/gsab_billing_key/archive'
log_filepath = '/rit_test/gsab_billing_key/log'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

time_zone = "utc"
schedule_interval = "0 */2 * * *"
first_delta = 3
second_delta = 0.5

job_created_since_time_delta_variable_name = "dxctechnology_gsap_billing_key_reprocess_job_created_since_time_delta_variable_name"
job_created_till_time_delta_variable_name = "dxctechnology_gsap_billing_key_reprocess_job_created_till_time_delta_variable_name"

reprocess_wbs_log_name = "gsap_billing_key_reprocess_log_wbs"

trigger_parallel_dagrun_count_project = 20

master_dag_max_active_runs = 1
child_process_wbs_max_runs = 10
child_dag_max_active_runs = 10
max_active_run_log_generation = 1

execution_timeout_days = 14

max_active_run_move_to_processing = 1

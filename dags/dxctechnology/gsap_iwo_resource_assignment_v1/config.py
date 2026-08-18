region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-RepliconIntGSAP'
sftp_conn_id = 'sftp_useast2'

move_file_input_filepath = '/rit_test/gsap_iwo_assignment/input'
input_filepath = '/rit_test/gsap_iwo_assignment/processing'
archive_filepath = '/rit_test/gsap_iwo_assignment/archive'
log_filepath = '/rit_test/gsap_iwo_assignment/log'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

extract_report_name = '**C1 Lean staffing Import base report'

master_dag_max_active_runs = 1
child_dag_max_active_runs = 10
execution_timeout_days = 14
trigger_parallel_dagrun_process_each_wbs_attribute = 20
gather_logs_timeout_hours = 12

utc_timezone = 'Etc/UTC'
schedule = '30 */3 * * *'
max_active_run_move_to_processing = 1

# Reprocessing Dags required details
time_zone = "utc"
schedule_interval = "0 */2 * * *"
first_delta = 3
second_delta = 0.5

reprocess_wbs_log_name = "gsap_iwo_resource_assignment_reprocess_log"
master_dag_max_active_runs=1

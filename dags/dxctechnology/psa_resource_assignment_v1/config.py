region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-replicon-RepliconIntPSA'
sftp_conn_id = 'rsftp-useast_for_testing'

input_filepath = '/rit_test/psa_resource/Input'
archive_filepath = '/rit_test/psa_resource/Archive'
log_filepath = '/rit_test/psa_resource/Logs'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

extract_report_name = '**C1 Lean staffing Import base report'

master_dag_max_active_runs = 1
child_dag_max_active_runs = 10
parallel_dagrun_count_each_wbs_attribute = 20
execution_timeout_days = 14

can_run_batch_task_var_name = 'psa_resource_assignment_batch_task_v1'

region = 'us-east-1'
environment = 'pre-production'
company_key = 'galaxyusopcoinctrial01'
replicon_conn_id = 'galaxyusopcoinctrial01_replicon_admin'
sftp_conn_id = 'sftp_useast2'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

pgp_conn_id = "pgp_vialto_partners"
master_dag_interval = 30
max_active_runs_process_employees = 10
max_active_runs_process_policy = 10
max_active_runs_process_log_generation = 1
child_process_execution_timeout = 14
child_wait_execution_timeout = 14
gather_employee_logs_timeout_hours = 2

trigger_parallel_dagrun_count_process_employees = 30

script_description = 'Set initial balance for the first day of a policy'
script_name = 'Starting Balance Set To'
delimiter = '|'

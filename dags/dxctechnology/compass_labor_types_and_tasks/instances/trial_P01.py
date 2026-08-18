region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'

instance = 'trial'
replicon_conn_id = 'dxctrial01-replicon-RepliconIntCompass'
sftp_conn_id = 'sftp_dxc_compass_labor_types_tasks'

max_dag_run_child_process = 5
dag_max_active_tasks = 128

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sub_erp_name = 'P01'
master_dag_id = f'dxctechnology_compass_labor_types_and_tasks_master_sftp_{sub_erp_name}_{instance}'
input_filepath = f'/DXC/compass_labor_types_and_tasks/{sub_erp_name}/Input'
archive_filepath = f'/DXC/compass_labor_types_and_tasks/{sub_erp_name}/Archive'
log_filepath = f'/DXC/compass_labor_types_and_tasks/{sub_erp_name}/Logs'
disabled = True

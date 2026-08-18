region = 'us-east-2'
environment = 'pre-production'
company_key = 'DXCSandbox'

instance = 'DXCSandbox'
sub_erp_name = 'NT4'

replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
sftp_conn_id = 'dxcsandbox-sftp-628172_Compass'

max_dag_run_child_process = 5
dag_max_active_tasks = 128

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


master_dag_id = f'dxctechnology_compass_labor_types_and_tasks_master_sftp_{sub_erp_name}_{instance}'

input_filepath = f'/Test/Inbound/COMPASSIWOLaborTypes&Tasks/{sub_erp_name}/Input'
archive_filepath = f'/Test/Inbound/COMPASSIWOLaborTypes&Tasks/{sub_erp_name}/Archive'
log_filepath = f'/Test/Inbound/COMPASSIWOLaborTypes&Tasks/{sub_erp_name}/Logs'

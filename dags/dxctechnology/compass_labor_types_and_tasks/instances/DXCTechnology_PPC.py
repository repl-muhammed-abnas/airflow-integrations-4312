region = 'us-east-2'
environment = 'production'
company_key = 'DXCTechnology'

instance = 'DXCTechnology'
sub_erp_name = 'PPC'

replicon_conn_id = 'dxctechnology-replicon-RepliconIntCompass'
sftp_conn_id = 'DXCTechnology-sftp-628172_COMPASS'

max_dag_run_child_process = 5
dag_max_active_tasks = 128

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


master_dag_id = f'dxctechnology_compass_labor_types_and_tasks_master_sftp_{sub_erp_name}_{instance}'

input_filepath = f'/Production/Inbound/COMPASSIWOLaborTypes&Tasks/{sub_erp_name}/Input'
archive_filepath = f'/Production/Inbound/COMPASSIWOLaborTypes&Tasks/{sub_erp_name}/Archive'
log_filepath = f'/Production/Inbound/COMPASSIWOLaborTypes&Tasks/{sub_erp_name}/Logs'

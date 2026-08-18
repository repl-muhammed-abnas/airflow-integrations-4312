# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.wf39_psa_resource_assignment_compass_v3.config import *

instance = 'trial'

region = 'us-east-2'
environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01_replicon_x.replicon.workday1'
sftp_conn_id = "sftp_useast2"

input_filepath = "/DXC/wf39_psa/compass_processing"
archive_filepath = "/DXC/wf39_psa/Archive"
log_filepath = "/DXC/wf39_psa/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

pgp_conn_id = 'pgp_dxcsandbox_psa_resource_assignment'

can_run_batch_task_var_name = f"dxc_wf39_psa_compass_resource_assignment_can_run_batch_task_{instance}"
can_decrypt_file_var_name = f'dxc_wf39_psa_compass_resource_assignment_can_decrypt_file_{instance}'

master_dagid = f'dxctechnology_wf39_psa_resource_assignment_compass_import_master_{instance}_v3'
billing_rate_child_dagid =f'dxctechnology_wf39_psa_resource_assignment_compass_create_billing_rate_child_{instance}_v3'
distinct_wbs_child_dagid= f'dxctechnology_wf39_psa_resource_assignment_compass_process_distinct_wbs_item_child_{instance}_v3'

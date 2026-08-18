# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.wf39_psa_resource_assignment_compass_v3.config import *

instance = 'sandbox'

region = 'us-east-2'
environment = 'pre-production'

company_key = 'dxcsandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntPSA'
sftp_conn_id = "sftp_dxctechnology_psa"

input_filepath = "/Test/Inbound/C1CP Resource Assignments/compass_processing"
archive_filepath = "/Test/Inbound/C1CP Resource Assignments/Archives"
log_filepath = "/Test/Inbound/C1CP Resource Assignments/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

pgp_conn_id = 'pgp_dxcsandbox_psa_resource_assignment'

can_run_batch_task_var_name = f"dxc_wf39_psa_compass_resource_assignment_can_run_batch_task_{instance}"
can_decrypt_file_var_name = f'dxc_wf39_psa_compass_resource_assignment_can_decrypt_file_{instance}'

master_dagid = f'dxctechnology_wf39_psa_resource_assignment_compass_import_master_{instance}_v3'
billing_rate_child_dagid =f'dxctechnology_wf39_psa_resource_assignment_compass_create_billing_rate_child_{instance}_v3'
distinct_wbs_child_dagid= f'dxctechnology_wf39_psa_resource_assignment_compass_process_distinct_wbs_item_child_{instance}_v3'

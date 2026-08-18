# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.wf39_psa_resource_assignment_v4.config import *

instance = 'sandbox2'
version = "_v4"

environment = 'pre-production'

company_key = 'dxcsandbox2'
replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntPSA'
sftp_conn_id = "sftp_dxcsandbox2_psa"

pgp_conn_id = 'pgp_dxcsandbox_psa_resource_assignment'

input_filepath = "/Test/Inbound/C1CP Resource Assignments/c1_processing"
archive_filepath = "/Test/Inbound/C1CP Resource Assignments/Archives"
log_filepath = "/Test/Inbound/C1CP Resource Assignments/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'dxctechnology_wf39_psa_resource_assignment_import_master_{instance}{version}'
child_dagid = f'dxctechnology_wf39_psa_resource_assignment_process_distinct_wbs_item_child_{instance}{version}'

can_run_batch_task_var_name = f"dxc_wf39_psa_c1_resource_assignment_can_run_batch_task_{instance}"
can_decrypt_file_var_name = f'dxc_wf39_psa_c1_resource_assignment_can_decrypt_file_{instance}'

# Skips re-applying records already in sync with Replicon, eliminating the
# redundant modification webhooks that cause duplicate C1 exports.
# Toggle the Airflow Variable below ('true'/'false') to enable/disable; rollback
# is instant with no redeploy. Other instances leave this unset (gate disabled).
idempotency_gate_var_name = f'dxc_wf39_psa_resource_assignment_idempotency_gate_{instance}'

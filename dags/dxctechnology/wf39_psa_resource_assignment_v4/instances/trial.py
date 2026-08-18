# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.wf39_psa_resource_assignment_v4.config import *

instance = 'trial'
version = "_v4"

environment = 'pre-production'

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01'
sftp_conn_id = "sftp_useast2"

input_filepath = "/DXC/wf39_psa/c1_processing"
archive_filepath = "/DXC/wf39_psa/Archive"
log_filepath = "/DXC/wf39_psa/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

pgp_conn_id = 'pgp_dxcsandbox_psa_resource_assignment'

master_dagid = f'dxctechnology_wf39_psa_resource_assignment_import_master_{instance}{version}'
child_dagid = f'dxctechnology_wf39_psa_resource_assignment_process_distinct_wbs_item_child_{instance}{version}'

can_run_batch_task_var_name = f"dxc_wf39_psa_c1_resource_assignment_can_run_batch_task_{instance}"
can_decrypt_file_var_name = f'dxc_wf39_psa_c1_resource_assignment_can_decrypt_file_{instance}'

# Idempotency / change-detection gate - TRIAL ONLY for now.
# Skips re-applying records already in sync with Replicon, eliminating the
# redundant modification webhooks that cause duplicate C1 exports.
# Toggle the Airflow Variable below ('true'/'false') to enable/disable; rollback
# is instant with no redeploy. Other instances leave this unset (gate disabled).
idempotency_gate_var_name = f'dxc_wf39_psa_resource_assignment_idempotency_gate_{instance}'

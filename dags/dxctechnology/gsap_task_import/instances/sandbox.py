# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_task_import.config import *

environment = 'pre-production'

company_key = 'dxcsandbox'
instance = "sandbox"

replicon_conn_id = 'dxcsandbox-replicon-RepliconIntGSAP'
sftp_conn_id = "sftp_dxctechnology_gsap"

input_filepath = "/Inbound/Tasks/Input"
archive_filepath = "/Inbound/Tasks/Archives/"
log_filepath = "/Inbound/Tasks/Logs/"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f"dxctechnology_gsap_task_import_can_run_batch_task_{instance}"

process_each_gsap_wbs_dagid = f"dxctechnology_gsap_task_import_process_each_gsap_wbs_childdag_{instance}"
process_each_gsap_wbs_billing_key_dagid = f"dxctechnology_gsap_task_import_process_gsap_billing_key_childdag_{instance}"
gsap_wbs_update_task_dagid = f"dxctechnology_gsap_task_import_update_gsap_task_childdag_{instance}"
gsap_wbs_create_task_dagid = f"dxctechnology_gsap_task_import_create_gsap_task_childdag_{instance}"

process_each_child_wbs_dagid = f"dxctechnology_gsap_task_import_process_each_gsap_child_wbs_childdag_{instance}"
process_each_child_wbs_billing_key_dagid = f"dxctechnology_gsap_task_import_process_gsap_child_wbs_billing_key_childdag_{instance}"
child_wbs_update_task_dagid = f"dxctechnology_gsap_task_import_update_gsap_child_wbs_task_childdag_{instance}"
child_wbs_create_task_dagid = f"dxctechnology_gsap_task_import_create_gsap_child_wbs_task_childdag_{instance}"

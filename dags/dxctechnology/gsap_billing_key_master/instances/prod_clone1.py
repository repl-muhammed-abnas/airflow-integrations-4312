# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_billing_key_master.config import *

region = 'us-east-2'
environment = 'production'
company_key = 'dxctechnology'
instance = 'production_clone1'
replicon_conn_id = 'dxctechnology_replicon_RepliconIntGSAP'
sftp_conn_id = "sftp_dxctechnology_gsap"

input_filepath_attr1 = "/Inbound/Billing Key/Processing1"
archive_filepath = "/Inbound/Billing Key/Archives"
log_filepath = "/Inbound/Billing Key/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_id_postfix = f'{instance}'

can_run_batch_task_var_name = f'dxctechnology_gsap_billing_key_{instance}_can_run_batch_task'

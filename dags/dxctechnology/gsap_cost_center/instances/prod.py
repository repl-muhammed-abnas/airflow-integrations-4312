# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_cost_center.config import *

environment = 'production'

instance = "production"
company_key = 'dxctechnology'

replicon_conn_id = 'dxctechnology_replicon_RepliconIntGSAP'
sftp_conn_id = 'sftp_dxctechnology_gsap'

input_filepath = "/Inbound/Cost Center/Input"
archive_filepath = "/Inbound/Cost Center/Archives"
log_filepath = "/Inbound/Cost Center/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'dxctechnology_gsap_cost_center_{instance}_can_run_batch_task'

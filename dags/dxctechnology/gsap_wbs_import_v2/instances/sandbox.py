# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_wbs_import_v2.config import *

environment = 'pre-production'

instance = 'sandbox'
company_key = 'dxcsandbox'

replicon_conn_id = 'dxcsandbox-replicon-RepliconIntGSAP'
sftp_conn_id = "sftp_dxctechnology_gsap"

input_filepath = "/Inbound/WBS/Processing"
archive_filepath = "/Inbound/WBS/Archives"
log_filepath = "/Inbound/WBS/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'dxctechnology_gsap_wbs_import_{instance}_can_run_batch_task'

disable=True

disabled=True

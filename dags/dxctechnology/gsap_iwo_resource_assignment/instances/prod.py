# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_iwo_resource_assignment.config import *

environment = 'production'
instance = 'production'

company_key = 'dxctechnology'

replicon_conn_id = 'dxctechnology_replicon_RepliconIntGSAP'
sftp_conn_id = "sftp_dxctechnology_gsap"

move_file_input_filepath = "/Inbound/IWO Assignment/Input"
input_filepath = "/Inbound/IWO Assignment/Processing"
archive_filepath = "/Inbound/IWO Assignment/Archives"
log_filepath = "/Inbound/IWO Assignment/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_id_postfix = f'{instance}'

can_run_batch_task_var_name = f'dxctechnology_gsap_iwo_resource_{instance}_can_run_batch_task'

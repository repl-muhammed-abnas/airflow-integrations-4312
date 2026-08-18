# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_iwo_resource_assignment.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = 'dxcsandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntGSAP'
sftp_conn_id = "sftp_dxctechnology_gsap"

move_file_input_filepath = "/Inbound/IWO Assignment/Input"
input_filepath = "/Inbound/IWO Assignment/Processing"
archive_filepath = "/Inbound/IWO Assignment/Archives"
log_filepath = "/Inbound/IWO Assignment/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_id_postfix = f'{instance}'

company_key = 'dxcsandbox'

can_run_batch_task_var_name = f'dxctechnology_gsap_iwo_resource_{instance}_can_run_batch_task'

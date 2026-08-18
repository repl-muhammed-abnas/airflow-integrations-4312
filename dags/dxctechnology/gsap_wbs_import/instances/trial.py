# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_wbs_import.config import *

instance = "trial"

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-replicon-RepliconIntGSAP'
sftp_conn_id = 'sftp_useast2'

input_filepath = "/Test/Inbound/GSAPWBS/Processing"
archive_filepath = "/Test/Inbound/GSAPWBS/Archive"
log_filepath = "/Test/Inbound/GSAPWBS/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'dxctechnology_gsap_wbs_import_{instance}_can_run_batch_task'

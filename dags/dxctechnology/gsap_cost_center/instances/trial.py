# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_cost_center.config import *

instance = "trial"

company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-replicon-RepliconIntGSAP'
sftp_conn_id = 'sftp_useast2'

input_filepath = "/Test/Inbound/GSAPCostCenter/Input"
archive_filepath = "/Test/Inbound/GSAPCostCenter/Archive"
log_filepath = "/Test/Inbound/GSAPCostCenter/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'dxctechnology_gsap_cost_center_{instance}_can_run_batch_task'

disable=True

disabled=True

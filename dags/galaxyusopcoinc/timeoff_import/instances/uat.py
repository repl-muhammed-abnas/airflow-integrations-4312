# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.timeoff_import.config import *

instance = "uat"
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'
input_filepath = '/Workday/LOA & RFL/Test/Input'
archive_filepath = '/Workday/LOA & RFL/Test/Archive'
log_filepath = '/Workday/LOA & RFL/Test/Log'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'vialtopartners_timeoff_import_run_batch_task_{instance}'
can_decrypt_file = f'vialtopartners_timeoff_import_can_decrypt_file_{instance}'

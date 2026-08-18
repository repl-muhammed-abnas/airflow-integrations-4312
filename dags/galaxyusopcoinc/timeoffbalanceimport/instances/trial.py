# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.timeoffbalanceimport.config import *

instance = "trial"
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'
input_filepath = '/Workday/Time Off Balance/Test/Input'
archive_filepath = '/Workday/Time Off Balance/Test/Archive'
log_filepath = '/Workday/Time Off Balance/Test/Log'
reference_file = "/Workday/Time Off Balance/Test/Reference/timeoff_balance_reference_file.csv"
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name= f'vialtopartners_timeoffbalance_importrun_batch_task_{instance}'
disabled = True

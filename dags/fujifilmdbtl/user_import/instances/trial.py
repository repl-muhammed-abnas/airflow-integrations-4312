# pylint: disable=wildcard-import unused-wildcard-import
from fujifilmdbtl.user_import.config import *

instance = "trial"
environment = 'pre-production'
company_key = 'FUJIFILMDBTLafmig'
replicon_conn_id = 'fujifilmdbtlafmig_replicon_repliconadmin'
sftp_conn_id = 'sftp_airflowmig_eucentral'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

input_filepath = '/fujifilmdbtl/UserSync'
reference_filepath = '/fujifilmdbtl/UserSync/reference'
archive_filepath = '/fujifilmdbtl/UserSync/Archive'
log_filepath = '/fujifilmdbtl/UserSync/logs'
ad_filepath = '/fujifilmdbtl/UserSync/ADFile'

can_use_reference_file = f'fujifilmdbtl_user_import_can_use_reference_file_{instance}'

can_run_batch_task_var_name = f'fujifilmdbtl_user_import_can_run_batch_task_{instance}'

# pylint: disable=wildcard-import unused-wildcard-import
from michaelkorstna.shift_import.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'MichaelKorsTnAafmig'
replicon_conn_id = 'replicon-MichaelKorsTnAafmig-radmin'
sftp_conn_id = 'Airflow_migration_SFTP_eucentral'

input_filepath = '/michaelkorstna/shift_import/input'
archive_filepath = '/michaelkorstna/shift_import/archive'
log_filepath = '/michaelkorstna/shift_import/logs'
shifts_filepath = '/michaelkorstna/shift_import/shifts'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'michaelkorstna_shift_import_{instance}_can_run_batch_task'

disabled=True

# pylint: disable=wildcard-import unused-wildcard-import
from npsg.timeoff_import.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'npsgafmig'
replicon_conn_id = 'npsgafmig_replicon_admin'
sftp_conn_id = 'Airflow_migration_SFTP_useast2'

input_filepath = '/npsg/timeoff_import/input'
archive_filepath = '/npsg/timeoff_import/archive'
log_filepath = '/npsg/timeoff_import/logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f'npsg_timeoff_import_{instance}_can_run_batch_task'
disabled = True

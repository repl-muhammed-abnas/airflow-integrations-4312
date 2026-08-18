# pylint: disable=wildcard-import unused-wildcard-import
from omd.singapore_timeoff_import.config import *

environment = 'pre-production'
instance = 'trial'
company_key = 'omdsingaporepteltdafmig'
replicon_conn_id = 'replicon-omdsingaporepteltdafmig-admin'
sftp_conn_id = 'Airflow_migration_SFTP_eucentral'

input_filepath = '/omd/singapore_timeoff_import/Input'
reference_filepath = '/omd/singapore_timeoff_import/Reference'
archive_filepath = '/omd/singapore_timeoff_import/Archive'
log_filepath = '/omd/singapore_timeoff_import/logs'
processing_filepath = '/omd/singapore_timeoff_import/processing'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'


can_run_batch_task_var_name = f'omd_timeoff_import_{instance}_can_run_batch_task'

disabled=True

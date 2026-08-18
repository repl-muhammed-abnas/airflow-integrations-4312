# pylint: disable=wildcard-import unused-wildcard-import
from omd.punch_time_import.config import *

instance = 'trial'
company_key = 'OMDSingaporePteLtdtrial01'


replicon_conn_id = 'omdsingaporepteltdtrial01_punch_time_import_adminr'
sftp_conn_id = 'sftp_internal'

input_filepath = '/omd/punch_time_import/Input'
archive_filepath = '/omd/punch_time_import/Archive'
log_filepath = '/omd/punch_time_import/logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'omdsingapore_punch_time_import_master_{instance}'
process_unique_users_child = f'omdsingapore_punch_time_import_process_unique_users_child_{instance}'
process_log_generation = f'omdsingapore_punch_time_import_process_log_generation_child_{instance}'

can_run_batch_task = f'omd_punch_time_import_{instance}_can_run_batch_task'
disabled = True

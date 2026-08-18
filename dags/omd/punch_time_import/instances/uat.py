# pylint: disable=wildcard-import unused-wildcard-import
from omd.punch_time_import.config import *

instance = 'uat'
company_key = 'OMDSingaporePteLtdtrial01'


replicon_conn_id = 'omdsingaporepteltdtrial01_punch_time_import_adminr'
sftp_conn_id = 'sftp_660053_omgindia'

input_filepath = '/Time Entries Import/Trial/Input'
archive_filepath = '/Time Entries Import/Trial/Archive'
log_filepath = '/Time Entries Import/Trial/Logs'

tenant_email = 'SmartTimesheet@omnicommediagroup.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'omdsingapore_punch_time_import_master_{instance}'
process_unique_users_child = f'omdsingapore_punch_time_import_process_unique_users_child_{instance}'
process_log_generation = f'omdsingapore_punch_time_import_process_log_generation_child_{instance}'

can_run_batch_task = f'omd_punch_time_import_{instance}_can_run_batch_task'

disabled=True

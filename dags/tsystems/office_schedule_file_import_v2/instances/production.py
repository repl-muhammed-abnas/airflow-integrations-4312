# pylint: disable=wildcard-import unused-wildcard-import
from tsystems.office_schedule_file_import_v2.config import *

region = 'eu-central-1'
instance = 'prod'
company_key = 'Tsystems'
environment = 'production'

replicon_conn_id = 'tsystems_replicon_repliconint.userimport'

sftp_conn_id = 'sftp_tsystems_Replicon_DarwinBox'

log_filepath = "/PROD/Schedule_WorkTime Import/LOGS"
input_filepath = "/PROD/Schedule_WorkTime Import/INPUT"
archive_filepath = "/PROD/Schedule_WorkTime Import/ARCHIVE"

tenant_email = 'TSI_Replicon@t-systems.com'
alert_email = "{{ var.value.dagrun_failure_alert_email }}"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

version = "v2"

can_run_batch_task_var_name = f'tsystems_office_schedule_file_import_{instance}_can_run_batch_task'

master_dag_id = f'tsystems_office_schedule_file_import_master_{instance}_{version}'
schedule_add_dag_id = f'tsystems_office_schedule_file_import_add_schedule_child_{instance}_{version}'
assign_schedule_dag_id = f'tsystems_office_schedule_file_import_assign_schedule_child_{instance}_{version}'

disabled = True
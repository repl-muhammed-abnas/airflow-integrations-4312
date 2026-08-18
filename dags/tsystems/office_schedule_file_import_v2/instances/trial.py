# pylint: disable=wildcard-import unused-wildcard-import
from tsystems.office_schedule_file_import_v2.config import *


instance = 'trial'
company_key = 'TsystemsSB'
environment = 'pre-production'

replicon_conn_id = 'TsystemsSB_replicon_replicon.admin'

sftp_conn_id = 'sftp_useast2'

log_filepath = "/TsystemsSB/office_schedule_file_import/Logs"
input_filepath = "/TsystemsSB/office_schedule_file_import/Input"
archive_filepath = "/TsystemsSB/office_schedule_file_import/Archive"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

version = "v2"

can_run_batch_task_var_name = f'tsystems_office_schedule_file_import_{instance}_can_run_batch_task'

master_dag_id = f'tsystems_office_schedule_file_import_master_{instance}_{version}'
schedule_add_dag_id = f'tsystems_office_schedule_file_import_add_schedule_child_{instance}_{version}'
assign_schedule_dag_id = f'tsystems_office_schedule_file_import_assign_schedule_child_{instance}_{version}'

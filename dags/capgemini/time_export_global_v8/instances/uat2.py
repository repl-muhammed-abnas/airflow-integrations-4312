# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.time_export_global_v8.config import *

instance = 'uat2'
environment = 'pre-production'

company_key = 'capgeminiuat2'

replicon_conn_id = 'capgeminiuat2_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_capgeminiuat2'

schedule_interval = "0 */3 * * *"

input_filepath = "/Outbound/GlobalTimedataUAT2/Input"
log_filepath = "/Outbound/GlobalTimedataUAT2/Logs"
s3_upload_filepath = "CapgeminiUAT/Outbound/GlobalTimedataUAT2/Input"

time_export_file_format = 'Global Data Hub Extract'
timesheet_period_base_user_location = "India"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

master_dag_id = f'capgemini_time_export_global_past_and_current_period_master_{instance}_v8'
time_export_child_dag_id = f'capgemini_time_export_global_create_export_child_{instance}_v8'

export_file_prefix = "Uat2"
can_send_time_export_downstream = f'capgemini_time_export_global_send_downstream_{instance}_v8'
can_run_batch_task_var_name = f'capgemini_time_export_global_v8_can_run_batch_task_{instance}'

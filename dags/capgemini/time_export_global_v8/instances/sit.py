# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.time_export_global_v8.config import *

instance = 'sit'
environment = 'pre-production'

company_key = 'capgeminisit'

replicon_conn_id = 'capgeminisit_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiSIT'
pgp_conn_id = 'pgp_capgeminisit'

schedule_interval = "0 */3 * * *"

input_filepath = "/Outbound/GlobalTimedata/Input"
log_filepath = "/Outbound/GlobalTimedata/Logs"
s3_upload_filepath = "CapgeminiSIT/Outbound/GlobalTimedata/Input"

time_export_file_format = 'Global Data Hub Extract'
timesheet_period_base_user_location = "India"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

master_dag_id = f'capgemini_time_export_global_past_and_current_period_master_{instance}_v8'
time_export_child_dag_id = f'capgemini_time_export_global_create_export_child_{instance}_v8'

export_file_prefix = "Sit"
can_send_time_export_downstream = f'capgemini_time_export_global_send_downstream_{instance}_v8'
can_run_batch_task_var_name = f'capgemini_time_export_global_v8_can_run_batch_task_{instance}'

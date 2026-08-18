# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.time_export_global_v9_pta.config import *

instance = 'uat2'
environment = 'pre-production'

company_key = 'capgeminiuat2'

replicon_conn_id = 'capgeminiuat2_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_capgeminiuat2'

input_filepath = "/Outbound/GlobalTimedataUAT2/Input"
log_filepath = "/Outbound/GlobalTimedataUAT2/Logs"
s3_upload_filepath = "CapgeminiUAT/Outbound/GlobalTimedataUAT2/Input"

time_export_file_format = 'Global Data Hub Extract'

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

export_file_prefix = "Uat2"
can_send_time_export_downstream = f'capgemini_time_export_global_pta_send_downstream_{instance}_v9'
can_run_batch_task_var_name = f'capgemini_time_export_global_v9_pta_can_run_batch_task_{instance}'

master_dagid = f'capgemini_time_export_global_v9_pta_master_{instance}'

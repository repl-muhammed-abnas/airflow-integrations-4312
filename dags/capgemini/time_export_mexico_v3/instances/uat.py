# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.time_export_mexico_v3.config import *

instance = 'uat'
environment = 'pre-production'

company_key = 'capgeminiuat'

replicon_conn_id = 'capgeminiuat_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_capgeminiuat'

input_filepath = "/Outbound/Timedata/Input"
log_filepath = "/Outbound/Timedata/Logs"
s3_upload_filepath = "CapgeminiUAT/Outbound/Timedata/Input"

timeoff_types_task_codes_mapper = "capgemini_time_export_timeoff_types_task_codes_mapper"

export_locations = "Mexico"
export_start_date = "2023/07/01"
time_export_file_format = 'GFS Extract Mexico'

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'
can_send_time_export_downstream = "capgeminiuat_time_export_mexico_send_downstream_v3"
master_dagid = f'capgemini_time_export_mexico_master_{instance}_v3'

disabled=True

# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.time_export_mexico_v3.config import *

instance = 'dev'
environment = 'pre-production'

company_key = 'capgeminidev'

replicon_conn_id = 'capgeminidev_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiDev'
pgp_conn_id = 'pgp_capgeminidev'

input_filepath = "/Outbound/Timedata/Input"
log_filepath = "/Outbound/Timedata/Logs"
s3_upload_filepath = "CapgeminiDev/Outbound/Timedata/Input"

timeoff_types_task_codes_mapper = "capgemini_time_export_timeoff_types_task_codes_mapper"

export_locations = "Mexico"
export_start_date = "2023/07/01"
time_export_file_format = 'GFS Extract Mexico'

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'
can_send_time_export_downstream = "capgeminidev_time_export_mexico_send_downstream_v3"
master_dagid = f'capgemini_time_export_mexico_master_{instance}_v3'

disabled=True

# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.time_export_v2.config import *

instance = 'sit'
environment = 'pre-production'

company_key = 'capgeminisit'

replicon_conn_id = 'capgeminisit_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiSIT'
pgp_conn_id = 'pgp_capgeminisit'

input_filepath = "/Outbound/Timedata/Input"
s3_upload_filepath = "CapgeminiSIT/Outbound/Timedata/Input"

timeoff_types_task_codes_mapper = "capgemini_time_export_timeoff_types_task_codes_mapper"

export_locations = "Mexico"
export_start_date = "2023/01/01"
time_export_file_format = 'GFS Extract Mexico'

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'
can_send_time_export_downstream = "capgeminisit_time_export_mexico_send_downstream_v2"

disabled = True

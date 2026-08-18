# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.time_export_global_v2.config import *

instance = 'uat'
environment = 'pre-production'

company_key = 'capgeminiuat'

replicon_conn_id = 'capgeminiuat_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_capgeminiuat'

input_filepath = "/Outbound/GlobalTimedata/Input"
s3_upload_filepath = "CapgeminiUAT/Outbound/GlobalTimedata/Input"

time_export_file_format = 'Global Data Hub Extract'
excepted_export_locations = "Mexico"
timesheet_period_base_user_location = "India"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

export_file_prefix = "Uat"
can_send_time_export_downstream = "capgeminiuat_time_export_global_send_downstream_v2"

# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.time_export_global_pta.config import *

instance = 'production'
environment = 'production'

company_key = 'capgemini'

replicon_conn_id = 'capgemini_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_Capgemini'
pgp_conn_id = 'pgp_capgemini'

input_filepath = "/Outbound/GlobalTimedata/Input"
s3_upload_filepath = "Capgemini/Outbound/GlobalTimedata/Input"

time_export_file_format = 'Global Data Hub Extract'
excepted_export_locations = "Mexico"

tenant_email = 'groupitrepliconsupportl2@capgemini.com,gtminterfacenotifications.hr@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }},capgeminisupportreplicon@deltek.com'

export_file_prefix = "Prod"
can_send_time_export_downstream = "capgemini_time_export_global_pta_send_downstream"

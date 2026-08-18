# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.time_export_global_pta.config import *

instance = 'dev'
environment = 'pre-production'

company_key = 'capgeminidev'

replicon_conn_id = 'capgeminidev_replicon_RepliconInt'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiDev'
pgp_conn_id = 'pgp_capgeminidev'

input_filepath = "/Outbound/GlobalTimedata/Input"
s3_upload_filepath = "CapgeminiDev/Outbound/GlobalTimedata/Input"

time_export_file_format = 'Global Data Hub Extract'
excepted_export_locations = "Mexico"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

export_file_prefix = "Dev"
can_send_time_export_downstream = "capgeminidev_time_export_global_pta_send_downstream"

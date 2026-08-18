# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.workday_user_sync.time_off_plan.config import *

instance = "production"
environment = "production"

company_key = 'GalaxyUSOpcoInc'
replicon_conn_id = "galaxyusopcoinc_replicon_admin"
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'
pgp_conn_id = "pgp_vialto_partners"


tenant_email = 'gbl_vialto_technology_digital_replicon_time_entry@vialto.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

report_filter_name = "OEFilter_UserOEF67ba3257e4fd46d18f035221b1d9aa32"

input_filepath = "/Workday/Time off Plan/Production/Input"
archive_filepath = "/Workday/Time off Plan/Production/Archive"
log_filepath = "/Workday/Time off Plan/Production/Log"
disabled = True

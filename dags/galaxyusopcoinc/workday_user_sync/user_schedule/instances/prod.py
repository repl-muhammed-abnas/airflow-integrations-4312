# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.workday_user_sync.user_schedule.config import *

instance = "production"
environment = "production"

company_key = 'GalaxyUSOpcoInc'
replicon_conn_id = "galaxyusopcoinc_replicon_admin"
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'

tenant_email = 'gbl_vialto_technology_digital_replicon_time_entry@vialto.com,utpal.chakraborty@vialto.com,hemanth.maru@vialto.com,farhan.afzal@vialto.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = "/Workday/Work Schedules/Prod/Input"
archive_filepath = "/Workday/Work Schedules/Prod/Archive"
log_filepath = "/Workday/Work Schedules/Prod/Log"
disabled = True

# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.timeoffexport.config import *

instance = "production"
environment = 'production'

company_key = 'GalaxyUSOpcoInc'

replicon_conn_id = 'galaxyusopcoinc_replicon_admin'
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'

sftp_upload_path = '/Workday/Time Off Booking/Prod/Input'
s3_upload_path = 'GalaxyUSOpcoInc/timeoff_export'

tenant_email = 'gbl_vialto_technology_digital_replicon_time_entry@vialto.com,utpal.chakraborty@vialto.com,hemanth.maru@vialto.com,farhan.afzal@vialto.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

timeoff_filter_query='''SELECT * from create_export_data_collection WHERE timeoffdescription NOT LIKE 'LOA/_%' ESCAPE '/'
AND timeoffdescription NOT IN ('[TOIL]','[STAT HOLIDAY]')'''

# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.timeoffexport.config import *

instance = "uat"
environment = 'pre-production'

company_key = 'galaxyusopcoinctrial01'

replicon_conn_id = 'galaxyusopcoinctrial01_replicon_admin'
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'

sftp_upload_path = '/Workday/Time Off Booking/Test/Input'
s3_upload_path = 'galaxyusopcoinctrial01/timeoff_export'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

timeoff_filter_query='''SELECT * from create_export_data_collection WHERE timeoffdescription NOT LIKE 'LOA/_%' ESCAPE '/'
AND timeoffdescription NOT IN ('[TOIL]','[STAT HOLIDAY]')'''

disabled=True

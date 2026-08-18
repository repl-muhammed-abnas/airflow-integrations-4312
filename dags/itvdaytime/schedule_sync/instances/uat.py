# pylint: disable=wildcard-import unused-wildcard-import
from itvdaytime.schedule_sync.config import *

instance = "uat"
environment = 'pre-production'
company_key = "itvdaytimetrial01"

BATCH_SIZE = 4000

upload_filepath = "/Trial/Export/Schedule"
sftp_conn_id = "sftp-itvdaytime-563217"

pgp_connection_id = f"pgp_{company_key}"

tenant_email = "technologyservicedesk@itv.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disabled= True

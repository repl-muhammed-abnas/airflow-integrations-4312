# pylint: disable=wildcard-import unused-wildcard-import
from itvdaytime.schedule_sync.config import *

region = "eu-central-1"
environment = 'production'

company_key = "ITVDaytime"
instance = "production"

BATCH_SIZE = 4000

schedule_sync_report_name = "***Work Schedule Details Report"

replicon_conn_id = "replicon_itvdaytime_radmin"
sftp_conn_id = "sftp_itvdaytime_563217"
pgp_connection_id = f"pgp_{company_key}"

upload_filepath = "/Production/Export/Schedule"

tenant_email = "technologyservicedesk@itv.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

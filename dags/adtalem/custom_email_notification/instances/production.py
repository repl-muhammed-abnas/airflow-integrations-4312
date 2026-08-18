# pylint: disable=wildcard-import unused-wildcard-import
from adtalem.custom_email_notification.config import *
instance = 'production'
region = 'us-east-1'
environment = 'production'
company_key = 'adtalem'
replicon_conn_id = 'adtalem-replicon-integration.user'


tenant_email = '{{ var.value.dagrun_internal_log_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_log_email }}'

schedule_interval = 30

report1_name = "***Timesheet Period Template***"
report2_name = "***Timesheet Notification Report***"
client_sftp_conn_id = "sftp_Integration_useast_prod"

log_path = "/Adtalem/customnotification/logs"

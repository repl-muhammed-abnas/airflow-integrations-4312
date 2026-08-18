# pylint: disable=wildcard-import unused-wildcard-import
from adtalem.custom_email_notification.config import *
instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'
company_key = 'adtalemafmig'
replicon_conn_id = "adtalem_replicon_migration"


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
bcc_tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

schedule_interval = 30

report1_name = "***Timesheet Period Template***"
report2_name = "***Timesheet Notification Report***"
client_sftp_conn_id = "client_horizon_sftp"

log_path = "/Adtalem/customnotification/logs"
disabled = True

# pylint: disable=wildcard-import unused-wildcard-import
from odessa.resource_allocation_removal_on_holidays.config import *

region = 'us-east-1'
instance = 'production'
environment = 'production'

company_key = 'Odessa'
replicon_conn_id = 'odessa-replicon-admin'

tenant_email = '{{ var.value.dagrun_internal_log_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_file_path = 'Odessa/removeresourceallocation/Logs/'

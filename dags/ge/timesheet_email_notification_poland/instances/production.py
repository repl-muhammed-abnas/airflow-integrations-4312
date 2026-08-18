# pylint: disable=wildcard-import unused-wildcard-import
from ge.timesheet_email_notification_poland.config import *

instance = 'production'
region = 'eu-central-1'
environment = 'production'

company_key = 'ge'
replicon_conn_id = 'ge_replicon_admin'

tenant_email = '{{ dag_run.conf.supervisor_data.supervisor_email }}'

s3_reference_key_name = 'GE/Poland/emailnotifications'

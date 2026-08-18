# pylint: disable=wildcard-import unused-wildcard-import
from ge.timesheet_email_notification_poland.config import *

instance = 'trial'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'GEtrial02'
replicon_conn_id = 'GEtrial02_replicon_admin'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

s3_reference_key_name = 'GEtrial02/Poland/emailnotifications'

disabled=True

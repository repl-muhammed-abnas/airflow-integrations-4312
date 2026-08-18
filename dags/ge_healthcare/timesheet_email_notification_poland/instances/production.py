# pylint: disable=wildcard-import unused-wildcard-import
from ge_healthcare.timesheet_email_notification_poland.config import *

instance = 'prod'
region = 'eu-central-1'
environment = 'production'

company_key = 'GEHealthcare'
replicon_conn_id = 'gehealthcare_replicon_admin'

tenant_email = '{{ dag_run.conf.supervisor_data.supervisor_email }}'

s3_reference_key_name = 'GE/Poland/emailnotifications'

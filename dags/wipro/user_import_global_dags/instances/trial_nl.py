# pylint: disable=wildcard-import unused-wildcard-import
from wipro.user_import_global_dags.config import *
instance = "trial"

region = 'eu-central-1'
environment = "pre-production"
time_zone = "Etc/UTC"
company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_replicon_myworkflow.Integration"

tenant_email = 'replicon.log.ext@wipro.com'
alerts_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

country = "Netherlands"
template_path = "templates/import_complete_mail_nl.html"

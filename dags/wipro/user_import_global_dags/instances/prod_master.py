# pylint: disable=wildcard-import unused-wildcard-import
from wipro.user_import_global_dags.config import *
instance = "prod"

region = 'eu-central-1'
environment = "production"
time_zone = "Etc/UTC"
company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_replicon_repliconint"

tenant_email = "replicon.log.ext@wipro.com"
alerts_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

country = "Poland"
template_path = "templates/import_complete_mail_pl.html"

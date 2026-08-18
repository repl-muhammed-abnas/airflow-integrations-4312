# pylint: disable=wildcard-import unused-wildcard-import
from cobalt.scheduled_time_sync_logs.config import *
environment = "production"
instance = "prod"
replicon_conn_id = "cobalt_replicon_casey.robinson"
company_key = "Cobalt"

tenant_email = 'RepliconZendeskIntegration@cobalt.net'
internal_logs_email = "{{ var.value.dagrun_internal_log_email }}"

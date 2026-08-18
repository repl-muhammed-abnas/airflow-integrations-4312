# pylint: disable=wildcard-import unused-wildcard-import
from cobalt.scheduled_time_sync_logs.config import *
environment = "pre-production"
instance = "trial"
replicon_conn_id = "cobalafmig_replicon_casey.robinson"
company_key = "Cobaltafmig"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

disabled=True

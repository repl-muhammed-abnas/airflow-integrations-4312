# pylint: disable=wildcard-import unused-wildcard-import
from matlensilver.user_sync_integration.disable_user.config import *

instance = 'trial'

company_key = 'repliconmatlentrial01'
replicon_conn_id = 'repliconmatlentrial01_replicon_admin'

master_dag_interval = '0 1 * * *'

tenant_email = 'IT@matlensilver.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True

# pylint: disable=wildcard-import unused-wildcard-import
from kmhavintegrationinc.custom_notification_for_task_milestone.config import *

region = 'us-east-1'
instance = "production"
environment = 'production'

company_key = 'KMHAVIntegrationInc'

replicon_conn_id = 'KMHAVIntegrationInc_replicon_khenneman'

tenant_email = "{{ result('get_adminmail_ids')}}" +','+ "{{ result('foreach_item_in_task_milestone_do')['Project Manager Email'] }}"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

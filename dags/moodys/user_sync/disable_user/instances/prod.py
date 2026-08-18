# pylint: disable=wildcard-import unused-wildcard-import
from moodys.user_sync.disable_user.config import *

instance = 'production'
environment = 'production'

company_key = 'MoodysEMEA'
replicon_conn_id = 'moodysemea_replicon_integrationuser'

master_dag_interval = '0 1 * * *'

master_dagid = f'moodys_user_sync_disable_user_master_{instance}'
child_dagid = f'moodys_user_sync_disable_user_child_{instance}'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

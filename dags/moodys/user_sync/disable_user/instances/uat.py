# pylint: disable=wildcard-import unused-wildcard-import
from moodys.user_sync.disable_user.config import *

instance = 'uat'

company_key = 'moodysemeatrial03'
replicon_conn_id = 'replicon_moodysemeatrial03_admin'

master_dag_interval = '0 1 * * *'

master_dagid = f'moodys_user_sync_disable_user_master_{instance}'
child_dagid = f'moodys_user_sync_disable_user_child_{instance}'

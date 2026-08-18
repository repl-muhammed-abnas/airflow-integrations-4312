# pylint: disable=wildcard-import unused-wildcard-import
from onepointapac.client_sync_singapore.config import *

instance = 'trial'
company_key = 'OnepointAPACafmig'
replicon_conn_id = 'Onepointapac_replicon_connid'
xero_conn_id = 'Onepointapac_xero_connid'

last_sync_time_var_name = f'onepointapac_client_sync_singapore_{instance}_last_sync_time'

master_dag_id = f'onepointapac_client_sync_singapore_master_{instance}'
child_dag_id = f'onepointapac_client_sync_singapore_child_{instance}'

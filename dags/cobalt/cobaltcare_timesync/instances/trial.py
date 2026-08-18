# pylint: disable=wildcard-import unused-wildcard-import
from cobalt.cobaltcare_timesync.config import *
environment = "pre-production"
instance = "trial"
replicon_conn_id = "cobalafmig_replicon_casey.robinson"
company_key = "Cobaltafmig"

time_sync_master_dag_id = f"cobaltcare_zendesk_to_replicon_timesync_master_{instance}"

can_redirect_to_workato_var_name = f'cobalt_time_sync_{instance}_redirect_to_workato'
workato_api_endpoint = f'cobalt_time_sync_{instance}_workato_endpoint'

disabled=True

# pylint: disable=wildcard-import unused-wildcard-import
from repliconinc.sync_deleted_timeoff_to_polaris.config import *

environment = 'production'
instance = "prod"
company_key = "deltekps"
company_key_polaris = "RepliconPinc"

replicon_conn_id_repliconinc ="deltekps-replicon-replicon.integration"
replicon_conn_id_polaris ="repliconpinc_replicon_replicon.integration_conn_id"

main_dag_id = f"repliconinc_sync_deleted_timeoff_to_polaris_master_{instance}"
delete_timeoff_entries_from_polaris_child= f"repliconinc_sync_deleted_timeoff_to_polaris_child_{instance}"




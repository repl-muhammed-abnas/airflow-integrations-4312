from repliconinc.sync_deleted_timeoff_to_polaris.config import *

instance = "trial"
environment = "pre-production"

company_key = "repliconinctrial01"
company_key_polaris = "Repliconpincstream6dev"

replicon_conn_id_polaris = "repliconinc_replicon_replicon.polaris"
replicon_conn_id_repliconinc = "repliconinc_replicon_replicon.integration"

main_dag_id = f"repliconinc_sync_deleted_timeoff_to_polaris_master_{instance}"
delete_timeoff_entries_from_polaris_child= f"repliconinc_sync_deleted_timeoff_to_polaris_child_{instance}"
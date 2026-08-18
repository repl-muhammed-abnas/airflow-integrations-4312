# pylint: disable=wildcard-import unused-wildcard-import
from repliconinc.timeoff_sync_to_polaris.config import *

environment = 'production'
instance = "prod"
company_key = "deltekps"
company_key_polaris = "RepliconPinc"

replicon_conn_id_repliconinc ="deltekps-replicon-replicon.integration"
replicon_conn_id_polaris ="repliconpinc_replicon_replicon.integration_conn_id"

main_dag_id = f"replicon_polaris_inc_report_polaris_master_{instance}"
push_timeoffentries_to_polaris= f"repliconpinc_sync_timeoffentries_to_polaris_child_{instance}"




from repliconinc.timeoff_sync_to_polaris.config import *

instance = "trial"
environment = "pre-production"

company_key = "test-airflow"
company_key_polaris = "Repliconpincstream6dev"

replicon_conn_id_polaris = "repliconinc_replicon_replicon.polaris"
replicon_conn_id_repliconinc = "repliconinc_replicon_replicon.integration"

main_dag_id = f"replicon_polaris_inc_report_polaris_master_{instance}"
push_timeoffentries_to_polaris= f"repliconPinc_sync_timeoffentries_to_polaris_child_{instance}"
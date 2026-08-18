from repliconinc.internal_monthly_shift_roster.config import *

# Instance-specific configuration
instance = "trial"
company_key = "RSEDeltek"
replicon_conn_id = f"{company_key}-replicon-stephenstanly"

# Version
version = "" # eg: _v1, _v2
dag_suffix = f"{instance}{version}"

# Dag configuration
master_dag_id = f"internal_monthly_shift_roster_master_{dag_suffix}"
process_each_user_child_dag_id = f"internal_monthly_shift_roster_process_each_user_child_{dag_suffix}"

# Webhook
bearer_token_var = f"repliconinc_internal_monthly_shift_roster_bearer_token_variable_{dag_suffix}"
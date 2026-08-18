# pylint: disable=wildcard-import unused-wildcard-import
from repliconinc.weekend_shift_assignment.config import *

environment = 'production'
instance = "prod"
company_key = "RepliconInc"

replicon_conn_id = "repliconinc_replicon_replicon.integration"

FROM_EMAIL_ADDR = "integrationteam@replicon.com"
TO_EMAIL_ADDR = "RepliconIntegrationTeam@deltek.com,dd4e5f09.deltekO365.onmicrosoft.com@amer.teams.ms,5a81ad83.deltekO365.onmicrosoft.com@amer.teams.ms"
CC_EMAIL_ADDR = "RaghuKandaswamy@deltek.com"

shift_name = "SaaS Weekend Night"


master_dag = f"weekend_shift_assignment_master_{instance}"
process_weekend_shift_records_child_dag = f"weekend_shift_assignment_process_weekend_shift_records_{instance}"
get_inhouse_shift_assignment_variable = f"inhouse_weekend_shift_assigment_{instance}_secret"

bearer_token_var = f"replicon_inhouse_weekend_shift_assignment_bearer_token_{instance}_secret"

# pylint: disable=wildcard-import unused-wildcard-import
from avenu.user_import.config import *

instance = 'uat02'
environment = 'pre-production'
input_filepath = '/Trial/User Import/Input'
archive_filepath = '/Trial/User Import/Archive'
log_filepath = '/Trial/User Import/Log'
reference_file = '/Trial/User Import/Reference/user_sync_reference_file.csv'

company_key = "avenuinsightsanalyticstrial02"

replicon_conn_id = 'avenuinsightsanalyticstrial02_replicon_admin'
sftp_conn_id = 'repliconsftp_avenu'

uat_variable_instance = "trial"
user_sync_mapper = f"avenu_user_sync_mapper_{uat_variable_instance}"
payrule_sync_mapper = f"avenu_payrule_sync_mapper_{uat_variable_instance}"

tenant_email = 'olivia.brennan@avenuinsights.com'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f"avenu_user_import_{uat_variable_instance}_can_run_batch_task"

disabled = True

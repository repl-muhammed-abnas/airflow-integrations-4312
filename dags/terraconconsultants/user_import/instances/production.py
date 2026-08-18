# pylint: disable=wildcard-import unused-wildcard-import
from terraconconsultants.user_import.config import *

instance = 'production'
environment = 'production'
company_key = 'terraconconsultants'

replicon_conn_id = 'TerraconConsultants_replicon_admin'
pgp_conn_id = 'pgp_Terraconconsultants_userimport'

tenant_email = 'timesheets@terracon.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_TerraconConsultants_626170PROD'

reference_key_name = 'TerraconConsultants/userimport/reference'
archive_key_name = 'TerraconConsultants/userimport/archive'
timeoff_mapper_key_name = 'TerraconConsultants/userimport/mappers/lookup_table_data_terracon-time-off-type-mapper.csv'

can_run_batch_task_var_name = f'terraconconsultants_user_import_{instance}_can_run_batch_task'

# pylint: disable=wildcard-import unused-wildcard-import
from terraconconsultants.user_import.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'terraconconsultantsafmig'

replicon_conn_id = 'replicon-terraconconsultantsafmig-admin'
pgp_conn_id = 'terraconconsultantsafmig_pgp_user_import'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_useast2'

reference_key_name = 'TerraconConsultantsafmig/userimport/reference'
archive_key_name = 'TerraconConsultantsafmig/userimport/archive'
timeoff_mapper_key_name = 'TerraconConsultantsafmig/userimport/mappers/lookup_table_data_terracon-time-off-type-mapper.csv'

can_run_batch_task_var_name = f'terraconconsultants_user_import_{instance}_can_run_batch_task'
disabled = True

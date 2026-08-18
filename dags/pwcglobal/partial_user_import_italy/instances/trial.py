# pylint: disable=wildcard-import unused-wildcard-import
from pwcglobal.partial_user_import_italy.config import *

region = 'eu-central-1'
instance = 'trial'
environment = 'pre-production'

company_key = 'PwCInternal'
replicon_conn_id = 'replicon_pwcinternal'
sftp_conn_id = "sftp_pwc_partial_user_import"

can_run_batch_task_var_name = f"PwC_partial_user_import_italy_can_run_batch_task_{instance}"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

input_filepath = 'pwcinternaltest/partial_user_import_italy/Input'
archive_filepath = 'pwcinternaltest/partial_user_import_italy/Archive'
log_filepath = 'pwcinternaltest/partial_user_import_italy/Logs'


partial_user_import_master_dag_id = f"PwC_partial_user_import_italy_master_{instance}"
process_add_update_custom_field_values_child_dag_id = f"PwC_partial_user_import_add_update_custom_field_values_child_{instance}"

disabled=True

# pylint: disable=wildcard-import unused-wildcard-import line-too-long
from pwcglobal.partial_user_import_italy.config import *

instance = 'PwCQA'
region = 'eu-central-1'
environment = 'pre-production'

company_key = 'PwCQA'

replicon_conn_id = 'pwcqa-replicon-eu.userimport'
sftp_conn_id = "pwcglobalqa-MFT-STG-replicon"

input_filepath = "/PwCGBL_RepliconGlobal_STG/QA/Inbound/Staff/Local/IT"
archive_filepath = "/PwCGBL_RepliconGlobal_STG/QA/Inbound/Staff/Local/IT/_archive"
log_filepath = "/PwCGBL_RepliconGlobal_STG/QA/Inbound/Staff/Local/IT/_logs"

tenant_email = "giacomo.caruso@pwc.com, antonio.oliva@pwc.com, francesca.gioia.minetto@pwc.com, bartosz.polawski@pwc.com, grzegorz.biernat@pwc.com, damian.korpas@pwc.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task_var_name = f"PwC_partial_user_import_italy_can_run_batch_task_{instance}"

partial_user_import_master_dag_id = f"PwC_partial_user_import_italy_master_{instance}"
process_add_update_custom_field_values_child_dag_id = f"PwC_partial_user_import_add_update_custom_field_values_child_{instance}"

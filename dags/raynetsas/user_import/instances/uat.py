# pylint: disable=wildcard-import unused-wildcard-import
from raynetsas.user_import.config import *
from raynetsas.user_import.mapper.country_code_mapper import COUNTRY_CODE_MAPPER, USER_DEFAULT_FIELDS

instance = "uat"
environment = "pre-production"

company_key = "Raynetsastrial01"

replicon_conn_id = "Raynetsastrial01_replicon_admin"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

process_user_child_dagid = f"raynetsas_user_import_child_{instance}"
process_each_user_dagid = f"raynetsas_user_import_process_each_user_child_{instance}"
can_run_batch_task_var_name = f"raynetsas_user_import_can_run_batch_task_var_{instance}"
country_code_mapper = COUNTRY_CODE_MAPPER
user_default_fields = USER_DEFAULT_FIELDS

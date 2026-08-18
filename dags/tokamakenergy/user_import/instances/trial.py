# pylint: disable=wildcard-import unused-wildcard-import
from tokamakenergy.user_import.config import *
from tokamakenergy.user_import.mappers.employee_fields import employee_fields_mapper_sandbox
region = 'eu-central-1'
instance = "trial"
environment = 'pre-production'
company_key = 'tokamakenergyltdtrial01'

bamboohr_domain = 'tokamakenergytest'
bamboohr_conn_id = 'tokamakenergyltdtrial01_bamboohr_conn_id'
replicon_conn_id = 'tokamakenergyltdtrial01_replicon_admin'
sumo_conn_id = 'sumologic-dagrunlogger'

master_dagid = f'tokamakenergy_user_import_master_{instance}'
create_user_child_dagid = f'tokamakenergy_user_import_create_user_child_{instance}'
update_user_child_dagid = f'tokamakenergy_user_import_update_user_child_{instance}'
process_user_child_dagid = f'tokamakenergy_user_import_process_each_user_child_{instance}'
disable_user_child_dagid = f'tokamakenergy_user_import_disable_user_child_{instance}'

create_user_legacy_child_dagid = f'tokamakenergy_user_import_create_user_legacy_child_{instance}'
can_run_batch_task_var_name = f'tokamakenergyltd_bamboohr_user_import_can_run_batch_task_{instance}'
last_synctime = f'tokamakenergyltd_bamboohr_user_import_last_synctime_{instance}'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

licenses = ["TimeOff Plus", "Polaris PSA"]

jobgrade_effective_date_field = 'customEffectiveDate'

required_employee_fields = employee_fields_mapper_sandbox

disabled = True

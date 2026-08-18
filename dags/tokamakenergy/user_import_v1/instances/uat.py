# pylint: disable=wildcard-import unused-wildcard-import
from tokamakenergy.user_import_v1.config import *
from tokamakenergy.user_import_v1.mappers.employee_fields import employee_fields_mapper_sandbox
region = 'eu-central-1'
instance = "uat"
environment = 'pre-production'
company_key = 'tokamakenergyltdtrial01'

bamboohr_domain = 'tokamakenergytest'
bamboohr_conn_id = 'tokamakenergyltdtrial01_bamboohr_conn_id'
replicon_conn_id = 'tokamakenergyltdtrial01_replicon_admin'
sumo_conn_id = 'sumologic-dagrunlogger'

version = 'v1'

master_dagid = f'tokamakenergy_user_import_master_{instance}_{version}'
create_user_child_dagid = f'tokamakenergy_user_import_create_user_child_{instance}_{version}'
update_user_child_dagid = f'tokamakenergy_user_import_update_user_child_{instance}_{version}'
process_user_child_dagid = f'tokamakenergy_user_import_process_each_user_child_{instance}_{version}'
disable_user_child_dagid = f'tokamakenergy_user_import_disable_user_child_{instance}_{version}'

create_user_legacy_child_dagid = f'tokamakenergy_user_import_create_user_legacy_child_{instance}_{version}'
can_run_batch_task_var_name = f'tokamakenergyltd_bamboohr_user_import_can_run_batch_task_{instance}_{version}'
last_synctime = f'tokamakenergyltd_bamboohr_user_import_last_synctime_{instance}'

tenant_email = 'P3MO@tokamakenergy.co.uk'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

licenses = ["TimeOff Plus", "Polaris PSA"]

jobgrade_effective_date_field = 'customEffectiveDate'

required_employee_fields = employee_fields_mapper_sandbox

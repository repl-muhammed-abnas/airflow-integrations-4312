# pylint: disable=wildcard-import unused-wildcard-import
from tokamakenergy.user_import.config import *
from tokamakenergy.user_import.mappers.employee_fields import employee_fields_mapper_production
region = 'eu-central-1'
instance = "production"
environment = 'production'
company_key = 'TokamakEnergyLtd'

bamboohr_domain = 'TokamakEnergy'
bamboohr_conn_id = 'tokamakenergyltd_bamboohr_conn_id'
replicon_conn_id = 'tokamakenergyltd_replicon_admin'
sumo_conn_id = 'sumologic-dagrunlogger'

master_dagid = f'tokamakenergy_user_import_master_{instance}'
create_user_child_dagid = f'tokamakenergy_user_import_create_user_child_{instance}'
update_user_child_dagid = f'tokamakenergy_user_import_update_user_child_{instance}'
process_user_child_dagid = f'tokamakenergy_user_import_process_each_user_child_{instance}'
disable_user_child_dagid = f'tokamakenergy_user_import_disable_user_child_{instance}'

create_user_legacy_child_dagid = f'tokamakenergy_user_import_create_user_legacy_child_{instance}'
can_run_batch_task_var_name = f'tokamakenergyltd_bamboohr_user_import_can_run_batch_task_{instance}'
last_synctime = f'tokamakenergyltd_bamboohr_user_import_last_synctime_{instance}'

tenant_email = 'p3mo@tokamakenergy.co.uk,peopleteam@tokamakenergy.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

licenses = ["TimeOff Plus", "Polaris PSA"]

jobgrade_effective_date_field = 'customEffectiveDate3'

required_employee_fields = employee_fields_mapper_production

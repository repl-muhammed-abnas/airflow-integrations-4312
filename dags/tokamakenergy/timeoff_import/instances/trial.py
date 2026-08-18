# pylint: disable=wildcard-import unused-wildcard-import
from tokamakenergy.timeoff_import.config import *
from tokamakenergy.timeoff_import.mapper.timeoff_mapper import timeoff_mapper_names

instance = 'trial'
environment = 'pre-production'

company_key = 'tokamakenergyltdtrial01'
company_domain = 'tokamakenergytest'

replicon_conn_id = 'tokamakenergyltdtrial01_replicon_admin'
bamboohr_conn_id = 'tokamakenergyltdtrial01_bamboohr_conn_id'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'

can_run_batch_task = f'tokamak_timeoff_sync_{instance}_can_run_batch_task'
last_run_date_var_name = f'tokamak_timeoff_sync_{instance}_last_run_date'
can_use_conf_payload_var_name = f'tokamak_timeoff_sync_{instance}_can_use_conf_payload'
timeoff_start_daterange_var_name = f'tokamak_timeoff_sync_{instance}_start_daterange'
timeoff_end_daterange_var_name = f'tokamak_timeoff_sync_{instance}_end_daterange'

master = f"tokamak_timeoff_sync_master_{instance}"
process_timeoff_child = f"tokamak_timeoff_sync_process_booking_timeoff_child_{instance}"
timeoff_booking_update_delete_child = f"tokamak_timeoff_sync_booking_update_delete_child_{instance}"
timeoff_add_child = f"tokamak_timeoff_sync_booking_add_child_{instance}"
TIMEOFF_MAPPER_NAMES = timeoff_mapper_names

# pylint: disable=wildcard-import unused-wildcard-import
from wipro.time_off_export.config import *

instance = "trial"
environment = "pre-production"

company_key = "Wiprosandbox2"
replicon_conn_id = "wiprosandbox2_replicon_repliconint"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

child_dag_id = f"wipro_time_off_export_process_payload_child_{instance}"
submit_timeoff_data_dag_id = f"wipro_time_off_export_submit_timeoff_child_{instance}"
log_master_dag_id = f"wipro_time_off_export_process_log_generation_master_{instance}"

lookup_log_timestamp_var = f'wipro_time_off_export_lookup_log_timestamp_{instance}'

disabled=True

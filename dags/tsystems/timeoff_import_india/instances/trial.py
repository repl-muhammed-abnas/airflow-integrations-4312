from tsystems.timeoff_import_india.config import *
from tsystems.timeoff_import_india.mapper.timeoff_mapper import timeoff_mapper

instance = "trial"

company_key = "tsystemsSB"
environment = "pre-production"

# SFTP Configuration
input_filepath = '/TsystemsSB/timeoff_import/india/input'
archive_filepath = '/TsystemsSB/timeoff_import/india/archive'
log_filepath = '/TsystemsSB/timeoff_import/india/logs'

# Email Configuration
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# Connection IDs
replicon_conn_id = "tsystems_replicon_replicon.admin"
sftp_conn_id = "sftp_useast2"

# DAG Configuration
version = ""  # _v1, _v2 etc.
dag_id_prefix = f"{instance}{version}"

# DAG IDs
master_dag_id = f"tsystems_timeoff_import_master_{dag_id_prefix}"
process_timeoff_child_dag_id = f"tsystems_timeoff_import_process_each_timeoff_booking_child_{dag_id_prefix}"

can_run_batch_task_var_name = f"tsystems_timeoff_import_india_run_batch_task"

TIMEOFF_TYPE_MAPPER = timeoff_mapper

disabled=True

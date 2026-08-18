from tsystems.timeoff_import_germany_iberia_v1.config import *
from tsystems.timeoff_import_germany_iberia_v1.mapper.timeoff_mapper import timeoff_mapper

instance = "uat"

company_key = "TSystemsSB"
environment = "pre-production"

# Email Configuration
tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

log_filepath = '/TEST/Time Off Import - Germany_Iberia'

# Connection IDs
replicon_conn_id = "tsystems_replicon_replicon.admin"

#Client SFTP Connection
sftp_conn_id = "sftp_tsystems_Replicon_Logs"

# DAG Configuration
version = "_v1"  # _v1, _v2 etc.
dag_id_suffix = f"{instance}{version}"

# DAG IDs
master_dag_id = f"tsystems_timeoff_import_germany_iberia_master_{dag_id_suffix}"
process_timeoff_child_dag_id = f"tsystems_timeoff_import_germany_iberia_process_each_record_child_{dag_id_suffix}"

can_run_batch_task_var_name = f"tsystems_timeoff_import_germany_iberia_run_batch_task"

TIMEOFF_TYPE_MAPPER = timeoff_mapper
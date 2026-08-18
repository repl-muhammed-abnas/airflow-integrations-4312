from tsystems.cost_center_hierarchy_import_v1.config import *

instance = "uat"

company_key = "tsystemsSB"

region = "eu-central-1"
environment = "pre-production"

# File paths and SFTP configuration
# CLIENT SFTP account
input_filepath = '/TEST/INPUT'
archive_filepath = '/TEST/ARCHIVE'
log_filepath = '/TEST/LOGS/'
sftp_conn_id = "sftp_tsystems_Replicon_BSO_Simunye"

# REPLICON SFTP ACCOUNT
reference_filepath = '/TSystems UAT/Replicon_BSO_Simunye/reference/'
reference_archive_filepath = '/TSystems UAT/Replicon_BSO_Simunye/reference/archive'

reference_sftp_conn_id = "sftp_tsystems_687552"

# Email notification settings
tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

replicon_conn_id = "tsystemssb_replicon_replicon.admin"


version = "_v1" # _v1, _v2 etc.

dag_id_suffix = f"{instance}{version}"

mast_dag_id = f"tsystems_cost_center_hierarchy_import_main_{dag_id_suffix}"
intermediate_dag_id = f"tsystems_cost_center_hierarchy_import_intermediate_child_{dag_id_suffix}"
add_cost_center_dag_id = f"tsystems_cost_center_hierarchy_import_add_cost_center_{dag_id_suffix}"
update_cost_center_dag_id = f"tsystems_cost_center_hierarchy_import_update_cost_center_{dag_id_suffix}"
manager_cost_center_restriction_update_dag_id = f"tsystems_cost_center_hierarchy_import_manager_update_{dag_id_suffix}"
log_generation_dag_id = f"tsystems_cost_center_hierarchy_import_log_generation_{dag_id_suffix}"


batch_task_var_name = "tsystems_cost_center_hierarchy_import_batch_task"

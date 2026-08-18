from tsystems.cost_center_hierarchy_import_v1.config import *

instance = "trial"

company_key = "tsystemsSB"

region = "eu-central-1"
environment = "pre-production"

# File paths and SFTP configuration
input_filepath = '/TsystemsSB/cost_center_hierarchy_import/'
archive_filepath = '/TsystemsSB/cost_center_hierarchy_import/archive/'
reference_filepath = '/TsystemsSB/cost_center_hierarchy_import/reference/'
reference_archive_filepath = '/TsystemsSB/cost_center_hierarchy_import/reference/archive'
log_filepath = '/TsystemsSB/cost_center_hierarchy_import/logs/'

# Email notification settings
tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_internal_testing_email }}"

replicon_conn_id = "tsystems_replicon_replicon.admin"
sftp_conn_id = "sftp_useast2"

reference_sftp_conn_id = "sftp_useast2"

version = "_v1" # _v1, _v2 etc.

dag_id_suffix = f"{instance}{version}"

mast_dag_id = f"tsystems_cost_center_hierarchy_import_main_{dag_id_suffix}"
intermediate_dag_id = f"tsystems_cost_center_hierarchy_import_intermediate_child_{dag_id_suffix}"
add_cost_center_dag_id = f"tsystems_cost_center_hierarchy_import_add_cost_center_{dag_id_suffix}"
update_cost_center_dag_id = f"tsystems_cost_center_hierarchy_import_update_cost_center_{dag_id_suffix}"
manager_cost_center_restriction_update_dag_id = f"tsystems_cost_center_hierarchy_import_manager_update_{dag_id_suffix}"
log_generation_dag_id = f"tsystems_cost_center_hierarchy_import_log_generation_{dag_id_suffix}"


batch_task_var_name = "tsystems_cost_center_hierarchy_import_batch_task"
disabled=True
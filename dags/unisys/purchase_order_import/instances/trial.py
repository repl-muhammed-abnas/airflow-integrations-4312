from unisys.purchase_order_import.config import *

instance = "trial"
company_key = "Unisysdev"

replicon_conn_id = "unisysdev_replicon_repliconint"

pgp_conn_id = "unisys_pgp_key"

# SFTP
input_file_path = "/unisys/purchase_order/input"
workday_input_filepath = "/unisys/workday/input"
sftp_conn_id = "sftp_useast2"
log_filepath = "/unisys/purchase_order/logs"

# Email configuration
tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_internal_testing_email }}"

# Dag identifiers
master_dag_id = f"unisys_purchase_order_import_master_{instance}"
process_department_groups_child_dag_id = f"unisys_purchase_order_ids_import_process_department_groups_child_{instance}"

can_decrypt_file_var_name = f"unisys_purchase_order_import_can_decrypt_file_{instance}"
can_run_batch_task = f"unisys_purchase_order_import_can_run_batch_task_{instance}"

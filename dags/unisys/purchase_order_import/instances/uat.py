from unisys.purchase_order_import.config import *

instance = "uat"
company_key = "UnisysUAT"

replicon_conn_id = "unisysuat_replicon_repliconint"

pgp_conn_id = "pgp_UnisysUAT_purchaseorderimport"

# SFTP
input_file_path = "/Inbound/Workday/Input"
workday_input_filepath = "/Inbound/Workday/WDImport"
sftp_conn_id = "sftp_unisysuat_710319_UAT"
log_filepath = "/Inbound/Workday/POLogs"

# Email configuration
tenant_email = "Prashant.Vishwakarma@unisys.com,Cynthia.Rachel@in.unisys.com,Unisysproject@deltek.com"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_internal_testing_email }}"

# Dag identifiers
master_dag_id = f"unisys_purchase_order_import_master_{instance}"
process_department_groups_child_dag_id = f"unisys_purchase_order_ids_import_process_department_groups_child_{instance}"

can_decrypt_file_var_name = f"unisys_purchase_order_import_can_decrypt_file_{instance}"
can_run_batch_task = f"unisys_purchase_order_import_can_run_batch_task_{instance}"

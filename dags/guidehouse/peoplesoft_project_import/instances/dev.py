# pylint: disable=wildcard-import unused-wildcard-import
from guidehouse.peoplesoft_project_import.config import *

instance = "dev"
environment = "pre-production"

version = ""

company_key = "GuideHouseIncSB2"

file_name_prefix = "PPS"

replicon_conn_id = "guidehouse_connid"
sftp_conn_id = "sftp_useast2"
pgp_conn_id = "pgp_encrypt_conn"
pgp_conn_id2 = "pgp_decrypt_conn"

input_filepath = "/guidehouse/ProjectImport/Input/"
archive_filepath = "/guidehouse/ProjectImport/Archive/"
sftp_log_path = "/guidehouse/ProjectImport/Logs/"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

main_dag_id = f"guidehouse_peoplesoft_project_import_main_{instance}{version}"
process_project_dag_id = f"guidehouse_peoplesoft_process_projects_child_{instance}{version}"
process_add_resource_dag_id = f"guidehouse_peoplesoft_process_add_resource_child_{instance}{version}"
create_division_dag_id = f"guidehouse_peoplesoft_create_division_child_{instance}{version}"

process_clients_dag_id = f"guidehouse_peoplesoft_process_clients_child_{instance}{version}"
child_dag_id_udf_update = f"guidehouse_peoplesoft_process_udf_child_{instance}{version}"

can_run_batch_task_var_name = f"guidehouse_peoplesoft_batch_task_enabled_{instance}"
can_decrypt_file_var_name = f"guidehouse_peoplesoft_can_decrypt_file_{instance}"

disabled = True
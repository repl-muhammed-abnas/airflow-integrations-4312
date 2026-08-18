# pylint: disable=wildcard-import unused-wildcard-import
from guidehouse.peoplesoft_project_import.config import *

region = "us-east-1"

instance = "uat"
environment = "pre-production"

version = ""

file_name_prefix = "PPS"

company_key = "GuideHouseIncSB"
replicon_conn_id = "GuideHouseIncSB-replicon-replicon.int"
sftp_conn_id = "sftp_guidehousesb2_678659_uat"
pgp_conn_id2 = "guidehousesb2_replicon_pgp_conn_inbound"

input_filepath = "/UAT/Inbound/PS Project and Workforce/Input/"
archive_filepath = "/UAT/Inbound/PS Project and Workforce/Archive/"
sftp_log_path = "/UAT/Inbound/PS Project and Workforce/Logs/"

tenant_email = 'guidehousedeltekprojectteam@deltek.com,ghcostpoint@guidehouse.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

main_dag_id = f"guidehouse_peoplesoft_project_import_main_{instance}{version}"
process_project_dag_id = f"guidehouse_peoplesoft_process_projects_child_{instance}{version}"
process_add_resource_dag_id = f"guidehouse_peoplesoft_process_add_resource_child_{instance}{version}"
process_clients_dag_id = f"guidehouse_peoplesoft_process_clients_child_{instance}{version}"
create_division_dag_id = f"guidehouse_peoplesoft_create_division_child_{instance}{version}"

can_run_batch_task_var_name = f"guidehouse_peoplesoft_batch_task_enabled_{instance}"
can_decrypt_file_var_name = f"guidehouse_peoplesoft_can_decrypt_file_{instance}"

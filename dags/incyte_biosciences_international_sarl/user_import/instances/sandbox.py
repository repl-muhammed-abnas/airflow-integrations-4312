# pylint: disable=wildcard-import unused-wildcard-import
from incyte_biosciences_international_sarl.user_import.config import *

instance = "sandbox"
environment = "pre-production"

company_key = "IBISSandbox"

replicon_conn_id = "ibissandbox_replicon_eshwar.kataiah"
sftp_conn_id = "sftp_ibissandbox_680616"
pgp_conn_id = "pgp_ibissandbox_user_import"

input_filepath = "/User Import/UAT/Input"
archive_filepath = "/User Import/UAT/Archive"
log_filepath = "/User Import/UAT/Log"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'incyte_user_import_master_{instance}'
process_groups_dagid = f'incyte_user_import_process_groups_child_{instance}'
process_new_countries_dagid = f'incyte_user_import_process_new_countries_child_{instance}'
process_new_departments_dagid = f'incyte_user_import_process_new_departments_child_{instance}'
process_new_work_location_dagid = f'incyte_user_import_process_new_work_locations_child_{instance}'
process_new_employee_types_dagid = f'incyte_user_import_process_new_employee_types_child_{instance}'
process_new_standard_hours_dagid = f'incyte_user_import_process_new_standard_hours_child_{instance}'
process_new_full_part_time_dagid = f'incyte_user_import_process_new_full_part_time_child_{instance}'
process_users_dagid = f'incyte_user_import_process_users_child_{instance}'
process_new_users_dagid = f'incyte_user_import_process_new_users_child_{instance}'
process_update_users_dagid = f'incyte_user_import_process_update_users_child_{instance}'
process_timeoff_type_assignment_new_user_dagid = f'incyte_user_import_process_timeoff_type_assignment_new_user_child_{instance}'
process_supervisor_dagid = f'incyte_user_import_process_pending_supervisor_child_{instance}'
process_log_generation_dagid = f'incyte_user_import_process_log_generation_child_{instance}'

can_run_batch_task_var_name = f'incyte_user_import_run_batch_task_{instance}'
can_decrypt_file_var_name = f'incyte_user_import_can_decrypt_file_{instance}'

disabled=True

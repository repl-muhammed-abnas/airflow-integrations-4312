# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.workday_user_sync.user_import_v2.config import *

instance = "uat"
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'

master_dag = f"vialtopartners_user_import_master_{instance}_v2"
division_dag_id = f"vialto_partners_user_import_add_division_{instance}_v2"
employee_type_dag_id = f"vialto_partners_user_import_add_employee_type_{instance}_v2"
user_add_dag_id = f"vialtopartners_user_import_add_user_child_{instance}_v2"
costcenter_dag_id = f"vialtopartners_user_import_costcenter_child_{instance}_v2"
department_dag_id = f"vialtopartners_user_import_department_child_{instance}_v2"
disable_user_dag_id = f"vialtopartners_user_import_disable_user_master_{instance}_v2"
location_dag_id = f"vialtopartners_user_import_location_child_{instance}_v2"
service_center_dag_id = f"vialtopartners_user_import_process_service_center_child_{instance}_v2"
process_groups_dag_id = f"vialto_partners_user_import_process_groups_{instance}_v2"
process_timeoff_dag_id = f"vialtopartners_user_import_process_time_off_policy_child_{instance}_v2"
add_zero_line_policy_dag_id = f"vialtopartners_user_import_update_user_add_zero_line_policy_{instance}_v2"
update_supervisor_dag_id = f"vialtopartners_user_import_update_supervisor_child_{instance}_v2"
user_update_dag_id = f"vialtopartners_user_import_update_user_child_{instance}_v2"
update_user_enddate_dag_id = f"vialtopartners_user_import_update_user_enddate_child_{instance}_v2"
user_dag_id = f"vialtopartners_user_import_user_child_{instance}_v2"
disable_user_child_dag_id = f"vialtopartners_user_import_disable_user_child_{instance}_v2"

can_decrypt_file_var_name = f"vialtopartners_user_import_can_decrypt_file_{instance}_v1"

user_report_name = "***User report***"
location_dag_max_active_runs = 1
department_dag_max_active_runs = 1
costcenter_dag_max_active_runs = 1
add_departments_max_active_runs = 1
dag_max_active_tasks = 128
user_dag_max_active_runs = 10
master_dag_max_active_runs = 1

disable_schedule = '@daily'

input_filepath = "/Workday/Demographics/Test/Input"
archive_filepath = "/Workday/Demographics/Test/Archive"
log_filepath = "/Workday/Demographics/Test/Logs"

delimiter = '|'
execution_timeout_hours = 12

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disabled=True

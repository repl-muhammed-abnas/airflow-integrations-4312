# pylint: disable=wildcard-import unused-wildcard-import
from nrdc.user_import_v3.config import *

instance = "NRDCafmig"
environment = 'pre-production'

company_key = 'nrdctrial01'
replicon_conn_id = 'nrdctrial01_UserImport'
can_run_batch_task_var_name = f'nrdc_user_import_usa_can_run_batch_task_{instance}'
user_report_name = '**User List For Email Notification**'
sftp_conn_id = "sftp_useast2"
sftp_conn_id2 = "sftp_useast2"

input_filepath = "/Trial_test/Input"
archive_filepath= "/Trial_test/Archive"
log_filepath = "/Trial_test/Logs"

execution_timeout_days = 14
child_dag_max_active_runs = 20
# Everyday at Eastern Time (US & Canada)  "hour": "05", "minute": "00" - 10 am UTC
schedule_interval = 30

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
user_import_report_name = '***User Import Reference'

version = "_v3"

nrdc_userimport_master = f"nrdc_user_import_master_{instance}{version}"
nrdc_updating_c3_c4_values = f"nrdc_user_import_process_update_user_child_{instance}{version}"
nrdc_updaterehiredisableuserbasicprofile = f"nrdc_user_import_process_rehire_user_child_{instance}{version}"
nrdc_basicaddupdate = f"nrdc_user_import_process_create_new_user_profile_child_{instance}{version}"
nrdc_assignsubstituteusersv2 = f"nrdc_user_import_process_substitute_users_child_{instance}{version}"
nrdc_add_user_v2 = f"nrdc_user_import_process_add_user_child_{instance}{version}"

c3_c4_profile_supervisors_variable = f"user_import_c3_c4_supervisors_{instance}"

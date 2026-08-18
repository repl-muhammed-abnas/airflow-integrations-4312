# pylint: disable=wildcard-import unused-wildcard-import
from nrdc.user_import_v1.config import *

instance = "NRDCafmig"
environment = 'pre-production'

company_key = 'nrdctrial01'
replicon_conn_id = 'nrdctrial01_UserImport'
can_run_batch_task_var_name = f'nrdc_user_import_usa_can_run_batch_task_{instance}'
user_report_name = '**User List For Email Notification**'
sftp_conn_id = "sftp_useast2"
sftp_conn_id2 = "sftp_useast2"

input_filepath = "/NRDCafmig/Input"
archive_filepath= "/NRDCafmig/Archive"

execution_timeout_days = 14
child_dag_max_active_runs = 20
# Everyday at Eastern Time (US & Canada)  "hour": "05", "minute": "00" - 10 am UTC
schedule_interval = 30
log_filepath = "/NRDCafmig/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
user_import_report_name = '***User Import Reference'

nrdc_userimport_master = f"nrdc_userimport_master_{instance}_v1"
nrdc_updating_c3_c4_values = f"nrdc_userimport_updating_c3_c4_values_child_{instance}_v1"
nrdc_updaterehiredisableuserbasicprofile = f"nrdc_userimport_updaterehiredisableuserbasicprofile_child_{instance}_v1"
nrdc_basicaddupdate = f"nrdc_userimport_basicaddupdate_child_{instance}_v1"
nrdc_assignsubstituteusersv2 = f"nrdc_userimport_assignsubstituteusers_child_{instance}_v1"
nrdc_add_user_v2 = f"nrdc_userimport_add_user_child_{instance}_v1"

c3_c4_profile_supervisors_variable = f"user_import_c3_c4_supervisors_{instance}"
disabled = True
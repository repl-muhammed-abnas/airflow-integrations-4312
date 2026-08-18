# pylint: disable=wildcard-import unused-wildcard-import
from orchestra.time_off_booking_import.config import *

instance = "uat"

company_key = "orchestragroupllctrial01"

replicon_conn_id = "orchestragroupllctrial01_replicon_admin"
sftp_conn_id = "sftp_orchestra_690486"

input_filepath = "/trial/Time Off Import/Input"
log_filepath = "/trial/Time Off Import/Log"
archive_filepath = "/trial/Time Off Import/Archive"
sftp_reference_filepath = "/trial/Time Off Import/Reference"

tenant_email = 'alison@inkhouse.com,jennifer.russell@orchestraco.com,anjali.thakkar@orchestraco.com,samina.banatwala@orchestraco.com,alex.dobranic@orchestraco.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

process_timeoff_import_master_dagid = f"orchestra_timeoff_booking_import_master_{instance}"
process_distinct_employees_dagid = f"orchestra_timeoff_booking_import_process_each_user_child_{instance}"
process_each_timeoff_dagid = f"orchestra_timeoff_booking_import_process_each_timeoff_child_{instance}"

can_run_batch_task_var_name = f'orchestra_timeoff_booking_import_run_batch_task_{instance}'

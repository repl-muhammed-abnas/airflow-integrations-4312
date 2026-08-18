# pylint: disable=wildcard-import unused-wildcard-import
from conduent.user_import.config import *
from conduent.user_import.mapper.general_mapper import general_user_attribute_mapper
from conduent.user_import.mapper.holiday_calendar_mapper import location_based_holiday_cal_mapper

instance = "uat"
environment = "pre-production"

company_key = "ConduentLLCSandbox"
replicon_conn_id = "ConduentLLCSandbox_replicon_int"
sftp_conn_id = "sftp_633276"

sftp_archive_path = "/Sandbox/User Import/Archive"
sftp_input_path = "/Sandbox/User Import/Input"
sftp_log_path = "/Sandbox/User Import/Log"

conduent_user_import_master = f"conduent_user_import_master_{instance}"
conduent_user_import_disable_users_child = f"conduent_user_import_disable_users_child_{instance}"
conduent_user_import_update_users_child = f"conduent_user_import_update_users_child_{instance}"
conduent_user_import_create_users_child = f"conduent_user_import_create_users_child_{instance}"
conduent_user_import_process_logs_child = f"conduent_user_import_process_logs_child_{instance}"

GENERAL_MAPPER = general_user_attribute_mapper
HOLIDAY_CALENDAR_MAPPER = location_based_holiday_cal_mapper

tenant_mail = "RepliconIntegrations@conduent.com"
internal_logs_mail ='{{ var.value.dagrun_internal_testing_email }}'
alert_mail = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = "conduent_user_import_can_run_batch_task"
disabled=True
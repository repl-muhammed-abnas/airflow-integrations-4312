# pylint: disable=wildcard-import unused-wildcard-import
from impervainc.user_sync.config import *
from impervainc.user_sync.mapper.imperva_mapper_table import imperva_mapper_table
from impervainc.user_sync.mapper.imperva_timezone_mapper import imperva_timezone_mapper

instance = 'prod'
company_key = 'ImpervaInc'

tenant_email = "replicon_import@imperva.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

replicon_conn_id = 'impervainc_replicon_admin'
http_conn_id = f"http_impervainc_user_sync_{instance}"
sftp_conn_id = "sftp_impervainc_user_import_prod"

imperva_usersync_master = f"imperva_user_sync_master_{instance}"
imperva_usersync_update = f"imperva_user_sync_update_child_{instance}"
imperva_usersync_add = f"imperva_user_sync_add_child_{instance}"
imperva_usersync_disable_user = f"imperva_user_sync_disable_user_child_{instance}"
imperva_department_and_cost_center_check_child = f"imperva_user_sync_department_and_cost_center_check_child_{instance}"
imperva_organization_custom_field_check_child = f"imperva_user_sync_organization_custom_field_check_child_{instance}"
imperva_state_iso_code_custom_field_check_child = f"imperva_user_sync_state_iso_code_custom_field_check_child_{instance}"
imperva_work_state_custom_field_check_child = f"imperva_user_sync_work_state_custom_field_check_child_{instance}"
imperva_country_iso_custom_field_check_child = f"imperva_user_sync_country_iso_custom_field_check_child_{instance}"
imperva_work_country_custom_field_check_child = f"imperva_user_sync_work_country_custom_field_check_child_{instance}"
imperva_time_type_custom_field_check_child = f"imperva_user_sync_time_type_custom_field_check_child_{instance}"
imperva_employee_type_custom_field_check_child = f"imperva_user_sync_employee_type_custom_field_check_child_{instance}"
imperva_worker_type_custom_field_check_child = f"imperva_user_sync_worker_type_custom_field_check_child_{instance}"
imperva_payrate_type_custom_field_check_child = f"imperva_user_sync_payrate_type_custom_field_check_child_{instance}"
imperva_user_sync_update_timeoff_assignment = f"imperva_user_sync_update_timeoff_assignment_child_{instance}"
imperva_user_sync_timeoff_add_user = f"imperva_user_sync_timeoff_add_user_child_{instance}"
imperva_put_remaining_balance_for_payout = f"imperva_user_sync_put_remaining_balance_for_payout_child_{instance}"
imperva_user_sync_update_rehire_time_off_type_child = f"imperva_user_sync_update_rehire_time_off_type_child_{instance}"
imperva_supervisor_assignment_child = f"imperva_user_sync_supervisor_assignment_child_{instance}"

can_use_conf_payload_var_name = f"impervainc_can_use_conf_payload_var_name_{instance}"

workday_report_endpoint="https://wd5-services1.myworkday.com/ccx/service/customreport2/imperva/ISU_Replicon/Replicon_Data_Sync?format=json"

rit_user_reference_report = "***rit_user_reference"
rit_dept_lookup_report = "**RIT-Department lookup"
input_filepath = "/input"
archive_filepath = "/archive"
reference_filepath = "/reference"
usersynclogs_filepath = "/usersynclogs"

imperva_payrule_placeholder = "*Imperva - Payrule Placeholder"
imperva_end_user_with_report_access = "**Imperva - End User with Report access"
imperva_supervisor = "**Imperva - Supervisor"
usereferencefile = "Yes"

can_run_batch_task_var_name = f"imperva_user_sync_can_run_batch_task_{instance}"
IMPERVA_MAPPER_TABLE = imperva_mapper_table
IMPERVA_TIMEZONE_MAPPER = imperva_timezone_mapper

disabled=True

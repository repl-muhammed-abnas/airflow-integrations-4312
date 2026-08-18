# pylint: disable=wildcard-import unused-wildcard-import
from crl.user_import_usa_v9.config import *
from crl.user_import_usa_v9.mappers.floating_holiday_placeholder_mapper_v1 import floating_holiday_timeoff_type_placeholder
from crl.user_import_usa_v9.mappers.vacation_time_off_type_placeholder_mapper import vacation_time_off_type_placeholder_mapper
from crl.user_import_usa_v9.mappers.sick_time_off_type_placeholder_mapper_v1 import sick_time_off_type_placeholder_mapper
from crl.user_import_usa_v9.mappers.user_import_mapper_v2 import user_import_mapper
from crl.user_import_usa_v9.mappers.timezone_mapper import timezone_mapper
from crl.user_import_usa_v9.mappers.special_timeoff_mapper import special_timeoff_type_accrual_mapper
from crl.user_import_usa_v9.mappers.custom_payrule_mapper import custom_payrule_mapper


instance = "prod"
environment = "production"

company_key = "CharlesRiverLaboratories"
replicon_conn_id = "CharlesRiverLaboratories_replicon_Repliconint_userimport"
sftp_conn_id = "sftp_charlesriverlaboratories_603355"

log_filepath = "/Production/Inbound/User Interface/Logs"
payload_filepath = "/Production/Inbound/User Interface/Archive"

# pylint: disable=line-too-long
tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,Prabhav.Potluri@crl.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

process_user_import_payload_dagid = f"crl_user_import_usa_process_each_payload_child_{instance}_v9"
process_users_dagid = f"crl_user_import_usa_process_users_child_{instance}_v9"
process_supervisor_dagid = f"crl_user_import_usa_process_pending_supervisor_child_{instance}_v9"
process_log_generation_dagid = f"crl_user_import_usa_process_log_generation_child_{instance}_v9"

process_groups_dagid = f"crl_user_import_usa_process_groups_child_{instance}_v9"
process_new_company_code_dagid = f"crl_user_import_usa_process_new_company_code_child_{instance}_v9"
process_new_locations_dagid = f"crl_user_import_usa_process_new_location_child_{instance}_v9"
process_new_buisness_unit_dagid = f"crl_user_import_usa_process_new_buisness_unit_child_{instance}_v9"
process_new_cost_center_dagid = f"crl_user_import_usa_process_new_cost_center_child_{instance}_v9"
process_new_department_dagid = f"crl_user_import_usa_process_new_department_child_{instance}_v9"

process_new_users_dagid = f"crl_user_import_usa_process_new_users_child_{instance}_v9"
process_update_users_dagid = f"crl_user_import_usa_process_update_users_child_{instance}_v9"
process_disable_users_dagid = f"crl_user_import_usa_process_disable_users_child_{instance}_v9"

process_timeoff_type_no_accrual_dagid = f"crl_user_import_usa_process_timeoff_type_no_accrual_child_{instance}_v9"
process_timeoff_type_assignment_new_user_dagid = f"crl_user_import_usa_process_timeoff_type_new_user_child_{instance}_v9"
process_timeoff_type_assignment_vacation_new_user_dagid = f"crl_user_import_usa_process_timeoff_type_vacation_new_user_child_{instance}_v9"
process_timeoff_type_assignment_update_rehire_user_dagid = f"crl_user_import_usa_process_timeoff_type_update_rehire_user_child_{instance}_v9"
process_timeoff_type_special_accrual_dagid = f"crl_user_import_usa_process_timeoff_type_special_accrual_child_{instance}_v9"


disable_user_master_dagid = f'crl_user_import_usa_disable_future_enddate_user_master_{instance}_v9'
disable_future_enddate_user_child_dagid = f'crl_user_import_usa_disable_future_enddate_user_child_{instance}_v9'

crl_user_import_bearer_token_var = f"crl_user_import_bearer_token_variable_{instance}"
can_run_batch_task_var_name = f'crl_user_import_run_batch_task_{instance}'

USER_MAPPER = user_import_mapper
TIMEZONE_MAPPER = timezone_mapper
FLOATING_HOLIDAY_TO_PLACEHOLDER = floating_holiday_timeoff_type_placeholder
VACATION_TO_PLACEHOLDER = vacation_time_off_type_placeholder_mapper
SICK_TO_PLACEHOLDER = sick_time_off_type_placeholder_mapper
SPECIAL_TIMEOFF_TYPES_ACCRUALS = special_timeoff_type_accrual_mapper
CUSTOM_PAYRULE_MAPPER = custom_payrule_mapper

INTEGRATION_USERNAME = 'integration_userimport, Replicon'

FLOATING_HOLIDAY_PLACEHOLDER_NAMES = {
    "placeholder_1": "[HOLLISTERHC][CCHC][Oklahomacity]",
    "placeholder_2": "[CRLHC3]",
    "placeholder_3": "[CRLHC1][SHC][ROCKHC][MORRHC][COGHC][FrdrkisHC][RHC]",
    "placeholder_4": "[MKPHC][ASCHC][SPENHC][MATTHC]",
    "placeholder_5": "[FrdrkrmsHouHC][DURFRDSKOHC]",
    "placeholder_6": "[NewarkHC]",
    "placeholder_7": "[KingHC]",
    "placeholder_8": "[RALHC][HHC]"
    }

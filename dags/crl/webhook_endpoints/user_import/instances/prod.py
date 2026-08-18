# pylint: disable=wildcard-import unused-wildcard-import
from crl.webhook_endpoints.user_import.config import *

instance = "prod"
environment = "production"

company_key = "CharlesRiverLaboratories"
replicon_conn_id = "CharlesRiverLaboratories_replicon_Repliconint_userimport"
sftp_conn_id = "sftp_charlesriverlaboratories_603355"

payload_filepath = "/Production/Inbound/User Interface/Archive"

# pylint: disable=line-too-long
tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,MTL-Payroll@crl.com,Shari.Guttman@crl.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

webhook_master_dagid = f"crl_user_import_webhook_master_{instance}"

process_split_country_wise_data_dagid = f"crl_user_import_split_location_records_child_{instance}"

process_non_live_location_records_dagid = f"crl_user_import_others_process_each_payload_child_{instance}"
process_canada_location_records_dagid = f"crl_user_import_canada_process_each_payload_child_{instance}_v7"
process_usa_location_records_dagid = f"crl_user_import_usa_process_each_payload_child_{instance}_v10"
process_mauritius_location_records_dagid = f"crl_user_import_mauritius_process_each_payload_child_{instance}_v2"
process_ireland_location_records_dagid = f"crl_user_import_ireland_process_each_payload_child_{instance}_v2"
process_uk_location_records_dagid = f"crl_user_import_uk_process_each_payload_child_{instance}"
process_brazil_location_records_dagid = f"crl_user_import_brazil_process_each_payload_child_{instance}"
process_israel_location_records_dagid = f"crl_user_import_israel_process_each_payload_child_{instance}"
process_switzerland_location_records_dagid = f"crl_user_import_switzerland_process_each_payload_child_{instance}"

can_process_mauritius_location_var = f"crl_user_import_can_process_mauritius_location_{instance}"
can_process_ireland_location_var = f"crl_user_import_can_process_ireland_location_{instance}"
can_process_uk_location_var = f"crl_user_import_can_process_uk_location_{instance}"
can_process_brazil_location_var = f"crl_user_import_can_process_brazil_location_{instance}"
can_process_israel_location_var = f"crl_user_import_can_process_israel_location_{instance}"
can_process_switzerland_location_var = f"crl_user_import_can_process_switzerland_location_{instance}"

crl_user_import_bearer_token_var = f"crl_user_import_bearer_token_variable_{instance}"
can_run_batch_task_var_name = f'crl_user_import_run_batch_task_{instance}'

NON_LIVE_COUNTRIES_QUERY = "(Location NOT LIKE 'CAN%' and Location NOT LIKE 'USA%' and Location NOT LIKE 'Mauritius%' and Location NOT LIKE 'IRL%' and Location NOT LIKE 'ISR%' and Location NOT LIKE 'CHE%' and Location NOT LIKE 'BRA%' and Location NOT LIKE 'GBR%')"

# pylint: disable=wildcard-import unused-wildcard-import
from wipro.webhooks.user_import.config import *
instance = "prod"

region = 'eu-central-1'
environment = "production"
time_zone = "Etc/UTC"
company_key = "WiproLimited"

replicon_conn_id = "wiprolimited_replicon_repliconint"

wipro_user_import_bearer_token_variable_trial = "wipro_user_import_bearer_token_variable_prod"
can_process_all_country_records="wiprolimited_can_process_all_country_records"
trigger_dag_id={
    "netherlands":f"wipro_user_import_process_users_netherlands_master_{instance}_v1",
    "saudi_arabia":f"wipro_user_import_process_users_saudi_arabia_master_{instance}_v2",
    "romania":f"wipro_user_import_process_users_romania_master_{instance}",
    "portugal":f"wipro_user_import_process_users_portugal_master_{instance}_v2",
    "poland":f"wipro_user_import_process_users_poland_master_{instance}_v1",
    "germany":f"wipro_user_import_process_users_germany_master_{instance}_v1",
    "ireland":f"wipro_user_import_process_users_ireland_master_{instance}_v1",
    "spain":f"wipro_user_import_process_users_spain_master_{instance}_v3",
    "united_kingdom":f"wipro_user_import_process_users_united_kingdom_master_{instance}_v2",
    "belgium":f"wipro_user_import_process_users_belgium_master_{instance}_v1",
    "switzerland":f"wipro_user_import_process_users_switzerland_master_{instance}",
    "austria":f"wipro_user_import_process_users_austria_master_{instance}_v1",
    "france":f"wipro_user_import_process_users_france_master_{instance}_v2"
}
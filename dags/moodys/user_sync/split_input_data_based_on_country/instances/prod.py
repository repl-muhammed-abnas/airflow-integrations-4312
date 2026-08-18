# pylint: disable=wildcard-import unused-wildcard-import
from moodys.user_sync.split_input_data_based_on_country.config import *

instance = "production"
environment = "production"

company_key = "MoodysEMEA"

replicon_conn_id = "moodysemea_replicon_integrationuser"
sftp_conn_id = "sftp_moodysemea_654601"
pgp_conn_id = "pgp_moodysemea_usersync"

input_filepath = "/MoodysEMEA/Prod/Usersync/Input"
archive_filepath = "/MoodysEMEA/Prod/Usersync/Archive"
log_filepath = "/MoodysEMEA/Prod/Usersync/Logs"

lithuania_processing_filepath = "/MoodysEMEA/Prod/Usersync/Processing/Lithuania"
costa_rica_processing_filepath = "/MoodysEMEA/Prod/Usersync/Processing/CostaRica"
united_states_processing_filepath = "/MoodysEMEA/Prod/Usersync/Processing/UnitedStates"
canada_processing_filepath = "/MoodysEMEA/Prod/Usersync/Processing/Canada"
france_processing_filepath = "/MoodysEMEA/Prod/Usersync/Processing/France"
japan_processing_filepath = "/MoodysEMEA/Prod/Usersync/Processing/Japan"
germany_processing_filepath = "/MoodysEMEA/Prod/Usersync/Processing/Germany"

# pylint: disable=line-too-long
tenant_email = "chanel.benjamin@moodys.com,globalpayrollintegration@moodys.com"

internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'moodys_user_sync_split_file_to_processing_each_country_master_{instance}'

can_decrypt_file_var_name = f'moodys_user_sync_can_decrypt_file_{instance}'

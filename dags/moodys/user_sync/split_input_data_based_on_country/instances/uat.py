# pylint: disable=wildcard-import unused-wildcard-import
from moodys.user_sync.split_input_data_based_on_country.config import *

instance = "uat"
environment = "pre-production"

company_key = "moodysemeatrial03"

replicon_conn_id = "replicon_moodysemeatrial03_admin"
sftp_conn_id = "sftp_moodysemeatrial02_654601"
pgp_conn_id = "pgp_moodysemeatrial02"

input_filepath = "/MoodysEMEA/UAT/Usersync/Input"
archive_filepath = "/MoodysEMEA/UAT/Usersync/Archive"
log_filepath = "/MoodysEMEA/UAT/Usersync/Logs"

lithuania_processing_filepath = "/MoodysEMEA/UAT/Usersync/Processing/Lithuania"
costa_rica_processing_filepath = "/MoodysEMEA/UAT/Usersync/Processing/CostaRica"
united_states_processing_filepath = "/MoodysEMEA/UAT/Usersync/Processing/UnitedStates"
canada_processing_filepath = "/MoodysEMEA/UAT/Usersync/Processing/Canada"
france_processing_filepath = "/MoodysEMEA/UAT/Usersync/Processing/France"
japan_processing_filepath = "/MoodysEMEA/UAT/Usersync/Processing/Japan"
germany_processing_filepath = "/MoodysEMEA/UAT/Usersync/Processing/Germany"

tenant_email = "chanel.benjamin@moodys.com,globalpayrollintegration@moodys.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'moodys_user_sync_split_file_to_processing_each_country_master_{instance}'

can_decrypt_file_var_name = f'moodys_user_sync_can_decrypt_file_{instance}'

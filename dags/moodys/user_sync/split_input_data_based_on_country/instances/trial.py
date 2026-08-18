# pylint: disable=wildcard-import unused-wildcard-import
from moodys.user_sync.split_input_data_based_on_country.config import *

instance = "trial"
environment = "pre-production"

company_key = "moodysemeatrial03"

replicon_conn_id = "replicon_moodysemeatrial03_admin"
sftp_conn_id = "sftp_internal_useast2"
pgp_conn_id = "pgp_moodysemeatrial02_usersync"

input_filepath = "/moodys/User Sync/Input"
archive_filepath = "/moodys/User Sync/Archive"
log_filepath = "/moodys/User Sync/Logs"

lithuania_processing_filepath = "/moodys/User Sync/Processing/Lithuania"
costa_rica_processing_filepath = "/moodys/User Sync/Processing/CostaRica"
united_states_processing_filepath = "/moodys/User Sync/Processing/UnitedStates"
canada_processing_filepath = "/moodys/User Sync/Processing/Canada"
france_processing_filepath = "/moodys/User Sync/Processing/France"
japan_processing_filepath = "/moodys/User Sync/Processing/Japan"
germany_processing_filepath = "/moodys/User Sync/Processing/Germany"


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

master_dagid = f'moodys_user_sync_split_file_to_processing_each_country_master_{instance}'

can_decrypt_file_var_name = f'moodys_user_sync_can_decrypt_file_{instance}'

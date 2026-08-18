# pylint: disable=wildcard-import unused-wildcard-import
from incyte_biosciences_international_sarl.time_off_sync.config import *

instance = "production"
environment = "production"

company_key = "Incyte"

replicon_conn_id = "ibisproduction_replicon_integrations.user"
sftp_conn_id = "sftp_ibisproduction_680616"
pgp_conn_id = "pgp_ibisproduction_time_off_sync"

sftp_import_path = "/Time off Bookings/Production/Input"
sftp_archive_path = "/Time off Bookings/Production/Archive/"
sftp_log_path = "/Time off Bookings/Production/Log/"

tenant_mail = "PS-Support-HR@incyte.com"
internal_logs_mail = '{{ var.value.dagrun_internal_log_email }}'
alert_mail = '{{ var.value.dagrun_failure_alert_email }}'
can_decrypt_file_var_name = f'incyte_time_off_sync_can_decrypt_file_{instance}'

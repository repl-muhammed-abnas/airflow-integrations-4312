# pylint: disable=wildcard-import unused-wildcard-import
from incyte_biosciences_international_sarl.time_off_sync.config import *

instance = "sandbox"
environment = "pre-production"

company_key = "IBISSandbox"

replicon_conn_id = "ibissandbox_replicon_eshwar.kataiah"
sftp_conn_id = "sftp_ibissandbox_680616"
pgp_conn_id = "pgp_ibissandbox_time_off_sync"

sftp_import_path = "/Time off Bookings/UAT/Input"
sftp_archive_path = "/Time off Bookings/UAT/Archive/"
sftp_log_path = "/Time off Bookings/UAT/Log/"

tenant_mail = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_mail = '{{ var.value.dagrun_internal_testing_email }}'
alert_mail = '{{ var.value.dagrun_failure_alert_email }}'
can_decrypt_file_var_name = f'incyte_time_off_sync_can_decrypt_file_{instance}'
disabled = True

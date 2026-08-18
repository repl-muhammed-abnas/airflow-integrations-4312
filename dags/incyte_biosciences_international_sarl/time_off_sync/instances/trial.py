# pylint: disable=wildcard-import unused-wildcard-import
from incyte_biosciences_international_sarl.time_off_sync.config import *

instance = "trial"
environment = "pre-production"

company_key = "ibistrial01"

replicon_conn_id = "replicon_ibis_trial"
sftp_conn_id = "sftp_useast2"
pgp_conn_id = "pgp_incyte_timeoff_sync_trial"

sftp_import_path = "/incyte/time_off_sync/input"
sftp_archive_path = "/incyte/time_off_sync/archive/"
sftp_log_path = "/incyte/time_off_sync/logs/"

tenant_mail = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_mail = '{{ var.value.dagrun_internal_testing_email }}'
alert_mail = '{{ var.value.dagrun_failure_alert_email }}'
can_decrypt_file_var_name = f'incyte_time_off_sync_can_decrypt_file_{instance}'
disabled = True

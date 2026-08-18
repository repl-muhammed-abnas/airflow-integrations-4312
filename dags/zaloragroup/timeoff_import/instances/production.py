# pylint: disable=wildcard-import unused-wildcard-import
from zaloragroup.timeoff_import.config import *

region = 'eu-central-1'
instance = "production"
environment = 'production'
company_key = 'zaloragroup'
replicon_conn_id = 'zaloragroup_replicon_admin'
sftp_conn_id = "sftp_zaloragroup_636673"
tenant_email = "frank.dadural@zalora.com,sumit.singh@my.zalora.com,umar.jamil@my.zalora.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
pgp_conn_id = "pgp_zaloragroup_timeoff_import"
input_filepath = '/Time Off Import'
input_filepath_master = '/Time Off Import/Processing'
upload_filepath = '/Time Off Import/Processing/'
archive_filepath = '/Time Off Import/Archive/'
log_filepath = '/Time Off Import/Logs/TimeoffImportLogs_'

#pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.iwo_perner_mapping.config import *

sub_erp_name ='PPC'
region = 'us-east-2'
environment = 'production'
instance = 'production'
company_key = 'dxctechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntCompass'
sftp_conn_id = "sftp_dxctechnology_compass"
input_filepath = f'/Production/Inbound/COMPASSIWOPernerMapping/{sub_erp_name}/Input'
archive_filepath = f'/Production/Inbound/COMPASSIWOPernerMapping/{sub_erp_name}/Archive'
log_filepath = f'/Production/Inbound/COMPASSIWOPernerMapping/{sub_erp_name}/Logs'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

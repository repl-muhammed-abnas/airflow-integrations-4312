#pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.iwo_perner_mapping.config import *

sub_erp_name ='NT3'
instance = 'dxcsandbox'
company_key = 'dxcsandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
sftp_conn_id = "dxcsandbox-sftp-628172_Compass"
input_filepath = f'/Test/Inbound/COMPASSIWOPernerMapping/{sub_erp_name}/Input'
archive_filepath = f'/Test/Inbound/COMPASSIWOPernerMapping/{sub_erp_name}/Archive'
log_filepath = f'/Test/Inbound/COMPASSIWOPernerMapping/{sub_erp_name}/Logs'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

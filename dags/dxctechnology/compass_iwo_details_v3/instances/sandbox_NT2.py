# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_iwo_details_v3.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = "dxcsandbox"
company_key = 'dxcsandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
sftp_conn_id = 'dxcsandbox-sftp-628172_Compass'
sub_erp = 'NT2'
input_filepath = '/Test/Inbound/COMPASSIWODetails/NT2/Input'
archive_filepath = '/Test/Inbound/COMPASSIWODetails/NT2/Archive'
log_filepath = '/Test/Inbound/COMPASSIWODetails/NT2/Logs'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_id_postfix = f'{instance}_{sub_erp}_v3'

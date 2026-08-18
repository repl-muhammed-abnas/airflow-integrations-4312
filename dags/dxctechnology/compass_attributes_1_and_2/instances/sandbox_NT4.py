# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_attributes_1_and_2.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = "dxcsandbox"
company_key = 'dxcsandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
sftp_conn_id = 'dxcsandbox-sftp-628172_Compass'
sub_erp = 'NT4'
input_filepath_attr1 = '/Test/Inbound/COMPASSAttributes1&2/NT4/InternalInput/Attribute1'
input_filepath_attr2 = '/Test/Inbound/COMPASSAttributes1&2/NT4/InternalInput/Attribute2'
archive_filepath = '/Test/Inbound/COMPASSAttributes1&2/NT4/Archive'
log_filepath = '/Test/Inbound/COMPASSAttributes1&2/NT4/Logs'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_id_postfix = f'{instance}_{sub_erp}'

# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_attribute_1_2_new_v1.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = "dxcsandbox2"
company_key = 'dxcsandbox2'
replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntCompass'
sub_erp = "NT4"
attribute = "Attribute_1"
sftp_conn_id = "dxcsandbox2-sftp-628172_Compass"
input_filepath = "/Test/Inbound/COMPASSAttributes1&2/Projectfields/Attribute1/NT4/Input"
archive_filepath = "/Test/Inbound/COMPASSAttributes1&2/Projectfields/Attribute1/NT4/Archive"
log_filepath = "/Test/Inbound/COMPASSAttributes1&2/Projectfields/Attribute1/NT4/Logs"
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

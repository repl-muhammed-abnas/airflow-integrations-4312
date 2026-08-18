# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_attribute_1_2_new.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = "dxcsandbox"
company_key = 'dxcsandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
sub_erp = "NT1"
attribute = "Attribute_1"
sftp_conn_id = "dxcsandbox-sftp-628172_Compass"
input_filepath = "/Test/Inbound/COMPASSAttributes1&2/Projectfields/Attribute1/NT1/Input"
archive_filepath = "/Test/Inbound/COMPASSAttributes1&2/Projectfields/Attribute1/NT1/Archive"
log_filepath = "/Test/Inbound/COMPASSAttributes1&2/Projectfields/Attribute1/NT1/Logs"
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

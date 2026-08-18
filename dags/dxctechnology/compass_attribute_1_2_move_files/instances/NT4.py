# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_attribute_1_2_move_files.config import *

region = 'us-east-2'
environment = 'pre-production'

company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
instance = "DXCSandbox"
sub_erp = "NT4"

sftp_conn_id = "dxcsandbox-sftp-628172_Compass"
input_filepath = "/Test/Inbound/COMPASSAttributes1&2/NT4/Input"
attribute1_filepath = "/Test/Inbound/COMPASSAttributes1&2/Projectfields/Attribute1/NT4/Input"
attribute2_filepath = "/Test/Inbound/COMPASSAttributes1&2/Projectfields/Attribute2/NT4/Input"

archive_filepath = "/Test/Inbound/COMPASSAttributes1&2/NT4/Archive"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

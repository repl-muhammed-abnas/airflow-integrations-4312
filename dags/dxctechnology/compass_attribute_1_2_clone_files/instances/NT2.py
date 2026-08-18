# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_attribute_1_2_clone_files.config import *

region = 'us-east-2'
environment = 'pre-production'

company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
instance = "DXCSandbox"
sub_erp = "NT2"

sftp_conn_id = "dxcsandbox-sftp-628172_Compass"
input_filepath = "/Test/Inbound/COMPASSAttributes1&2/NT2/Input"
copy_filepath_1 = "/Test/Inbound/COMPASSAttributes1&2/NT2/InternalInput"
attribute1_filepath = "/Test/Inbound/COMPASSAttributes1&2/Projectfields/Attribute1/NT2/Input"
attribute2_filepath = "/Test/Inbound/COMPASSAttributes1&2/Projectfields/Attribute2/NT2/Input"

attribute1_old_filepath = "/Test/Inbound/COMPASSAttributes1&2/NT2/InternalInput/Attribute1"
attribute2_old_filepath = "/Test/Inbound/COMPASSAttributes1&2/NT2/InternalInput/Attribute2"

archive_filepath = "/Test/Inbound/COMPASSAttributes1&2/NT2/Archive"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disable=True

disabled=True

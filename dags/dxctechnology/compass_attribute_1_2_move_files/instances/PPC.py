# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_attribute_1_2_move_files.config import *

region = 'us-east-2'
environment = 'production'

company_key = 'DXCTechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntCompass'
instance = "production"
sub_erp = "PPC"

sftp_conn_id = "sftp_dxctechnology_compass"
input_filepath = "/Production/Inbound/COMPASSAttributes1&2/PPC/Input"
attribute1_filepath = "/Production/Inbound/COMPASSAttributes1&2/Projectfields/Attribute1/PPC/Input"
attribute2_filepath = "/Production/Inbound/COMPASSAttributes1&2/Projectfields/Attribute2/PPC/Input"
archive_filepath = "/Production/Inbound/COMPASSAttributes1&2/PPC/Archive"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

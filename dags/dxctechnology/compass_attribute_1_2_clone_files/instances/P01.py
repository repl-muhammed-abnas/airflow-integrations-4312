# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_attribute_1_2_clone_files.config import *

region = 'us-east-2'
environment = 'production'

company_key = 'DXCTechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntCompass'
instance = "production"
sub_erp = "P01"

sftp_conn_id = "sftp_dxctechnology_compass"
input_filepath = "/Production/Inbound/COMPASSAttributes1&2/P01/Input"
copy_filepath_1 = "/Production/Inbound/COMPASSAttributes1&2/P01/InternalInput"
attribute1_filepath = "/Production/Inbound/COMPASSAttributes1&2/Projectfields/Attribute1/P01/Input"
attribute2_filepath = "/Production/Inbound/COMPASSAttributes1&2/Projectfields/Attribute2/P01/Input"

attribute1_old_filepath = "/Production/Inbound/COMPASSAttributes1&2/P01/InternalInput/Attribute1"
attribute2_old_filepath = "/Production/Inbound/COMPASSAttributes1&2/P01/InternalInput/Attribute2"

archive_filepath = "/Production/Inbound/COMPASSAttributes1&2/P01/Archive"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

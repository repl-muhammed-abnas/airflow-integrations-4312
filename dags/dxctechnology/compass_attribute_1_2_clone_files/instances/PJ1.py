# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_attribute_1_2_clone_files.config import *

region = 'us-east-2'
environment = 'production'

company_key = 'DXCTechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntCompass'
instance = "production"
sub_erp = "PJ1"

sftp_conn_id = "sftp_dxctechnology_compass"
input_filepath = "/Production/Inbound/COMPASSAttributes1&2/PJ1/Input"
copy_filepath_1 = "/Production/Inbound/COMPASSAttributes1&2/PJ1/InternalInput"
attribute1_filepath = "/Production/Inbound/COMPASSAttributes1&2/Projectfields/Attribute1/PJ1/Input"
attribute2_filepath = "/Production/Inbound/COMPASSAttributes1&2/Projectfields/Attribute2/PJ1/Input"
attribute1_old_filepath = "/Production/Inbound/COMPASSAttributes1&2/PJ1/InternalInput/Attribute1"
attribute2_old_filepath = "/Production/Inbound/COMPASSAttributes1&2/PJ1/InternalInput/Attribute2"
archive_filepath = "/Production/Inbound/COMPASSAttributes1&2/PJ1/Archive"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_attribute_1_2_new_v1.config import *

region = 'us-east-2'
environment = 'production'
instance = "production"
company_key = 'dxctechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntCompass'
sub_erp = "PJ1"
attribute = "Attribute_2"
sftp_conn_id = "sftp_dxctechnology_compass"
input_filepath = "/Production/Inbound/COMPASSAttributes1&2/Projectfields/Attribute2/PJ1/Input"
archive_filepath = "/Production/Inbound/COMPASSAttributes1&2/Projectfields/Attribute2/PJ1/Archive"
log_filepath = "/Production/Inbound/COMPASSAttributes1&2/Projectfields/Attribute2/PJ1/Logs"
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_attributes_1_and_2.config import *

region = 'us-east-2'
environment = 'production'
instance = 'prod'
company_key = 'dxctechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntCompass'
sub_erp = 'PPC'
sftp_conn_id = 'sftp_dxctechnology_compass'
input_filepath_attr1 = '/Production/Inbound/COMPASSAttributes1&2/PPC/InternalInput/Attribute1'
input_filepath_attr2 = '/Production/Inbound/COMPASSAttributes1&2/PPC/InternalInput/Attribute2'
log_filepath = '/Production/Inbound/COMPASSAttributes1&2/PPC/Logs'
archive_filepath = '/Production/Inbound/COMPASSAttributes1&2/PPC/Archive'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_id_postfix = f'{instance}_{sub_erp}'

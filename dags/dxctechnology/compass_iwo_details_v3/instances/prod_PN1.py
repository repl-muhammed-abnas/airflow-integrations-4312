# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_iwo_details_v3.config import *

region = 'us-east-2'
environment = 'production'

instance = 'prod'

company_key = 'dxctechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntCompass'

sub_erp = 'PN1'

sftp_conn_id = 'DXCTechnology-sftp-628172_COMPASS'

input_filepath = '/Production/Inbound/COMPASSIWODetails/PN1/Input'
log_filepath = '/Production/Inbound/COMPASSIWODetails/PN1/Logs'
archive_filepath = '/Production/Inbound/COMPASSIWODetails/PN1/Archive'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dag_id_postfix = f'{instance}_{sub_erp}_v3'

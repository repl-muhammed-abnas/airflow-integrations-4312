# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_gsap_billing_and_tasks.config import *

company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
sftp_conn_id = 'dxcsandbox-sftp-628172_Compass'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sub_erp_name = 'NT2'
input_filepath = f'/Test/Inbound/COMPASSGSAPBillingKey&Task/{sub_erp_name}/Input'
archive_filepath = f'/Test/Inbound/COMPASSGSAPBillingKey&Task/{sub_erp_name}/Archive'
log_filepath = f'/Test/Inbound/COMPASSGSAPBillingKey&Task/{sub_erp_name}/Logs'

debug = False

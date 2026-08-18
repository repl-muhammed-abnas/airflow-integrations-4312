# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_gsap_billing_and_tasks.config import *

company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
sftp_conn_id = 'dxcsandbox-sftp-628172_Compass'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sub_erp_name = 'NT1'
input_filepath = f'/Test/Inbound/COMPASSGSAPBillingKey&Task/{sub_erp_name}/Input'
archive_filepath = f'/Test/Inbound/COMPASSGSAPBillingKey&Task/{sub_erp_name}/Archive'
log_filepath = f'/Test/Inbound/COMPASSGSAPBillingKey&Task/{sub_erp_name}/Logs'

debug = False
if debug:
    replicon_conn_id = 'dxctrial01'
    input_filepath = 'import'
    archive_filepath = 'archive/CompassGSAP'
    log_filepath = 'logs/CompassGSAP'
    max_concurrent_wbs_imports = 1
    max_concurrent_gsap_task_imports = 1
    max_concurrent_billingkey_task_imports = 1

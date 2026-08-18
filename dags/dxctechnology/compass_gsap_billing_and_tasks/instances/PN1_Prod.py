# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_gsap_billing_and_tasks.config import *

sub_erp_name = 'PN1'
region = 'us-east-2'
environment = 'production'
instance = 'production'
company_key = 'dxctechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntCompass'
sftp_conn_id = "sftp_dxctechnology_compass"
gsap_report_name = 'GSAPbillingKeytask_basereport'
max_concurrent_billingkey_task_imports = 30
max_concurrent_gsap_task_imports = 30
input_filepath = f'/Production/Inbound/COMPASSGSAPBillingKey&Task/{sub_erp_name}/Input'
archive_filepath = f'/Production/Inbound/COMPASSGSAPBillingKey&Task/{sub_erp_name}/Archive'
log_filepath = f'/Production/Inbound/COMPASSGSAPBillingKey&Task/{sub_erp_name}/Logs'
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.ftp_sales_order_import.config import *

instance = 'dxcsandbox2'
company_key = 'dxcsandbox2'
replicon_conn_id = 'dxcsandbox2-replicon-RepliconIntFTP'
sftp_conn_id = "dxcsandbox2-sftp-628172_FTP"
input_filepath = "/Test/Inbound/FTPServiceOrders/Input"
archive_filepath = "/Test/Inbound/FTPServiceOrders/Archive"
log_filepath = "/Test/Inbound/FTPServiceOrders/Logs"
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

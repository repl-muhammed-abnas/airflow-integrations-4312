# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.ftp_sales_order_import.config import *

environment = 'production'
instance = 'DXCTechnology'
company_key = 'DXCTechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntFTP'
sftp_conn_id = "dxctechnology-sftp-628172_FTP"
input_filepath = "/Production/Inbound/FTPServiceOrders/Input"
archive_filepath = "/Production/Inbound/FTPServiceOrders/Archive"
log_filepath = "/Production/Inbound/FTPServiceOrders/Logs"
tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

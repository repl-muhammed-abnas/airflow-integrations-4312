# pylint: disable=wildcard-import unused-wildcard-import
from philipmorrisny.overtime_report_extract.config import *

instance = 'production'
environment = 'production'

company_key = 'philipmorrisny'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

replicon_conn_id= 'philipmorrisny-replicon-achauhan'
sftp_conn_id = 'philipmorrisny-sftp-509694'

overtime_report_name = 'Overtime Report'
overtime_log_report_name = 'Overtime Log Report'


overtime_report_path = '/Production/Overtime_Report/Export'
overtime_reportarchivepath = '/Production/Overtime_Report/Archive'
overtime_logreport_filepath = '/Production/Logs/Log_file'
overtime_logarchivepath = '/Production/Logs/Archive'

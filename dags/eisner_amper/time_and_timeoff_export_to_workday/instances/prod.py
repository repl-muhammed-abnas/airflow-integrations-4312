#pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.time_and_timeoff_export_to_workday.config import *

instance = "production"
environment = 'production'
company_key = 'EisnerAmper'
replicon_conn_id = 'eisneramper_repliconint.exports'
sftp_conn_id = "sftp_eisneramper_WS1095A"
sftp_conn_internal_id = "sftp_eisneramper_521759"
client_time_export_path = "/TimeBlock/"
internal_time_export_path = "/Production/Time Data to Workday/Time Block/"
client_timeoff_export_path = "/TimeOff/"
internal_timeoff_export_path = "/Production/Time Data to Workday/Time Off/"


tenant_email = "ashwin.ns@infosys.com,sap.alert.replicon@eisneramper.com,prasad.hukkeri@eisneramper.com,sap.integration.support@eisneramper.com,sap.proserv.support@eisneramper.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

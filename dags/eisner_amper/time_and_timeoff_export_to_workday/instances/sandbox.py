#pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.time_and_timeoff_export_to_workday.config import *

instance = "sandbox"
company_key = 'EisnerAmperSandbox'
replicon_conn_id = 'eisnerampersandbox_replicon_radmin'
sftp_conn_id = "sftp_eisnerampersandbox_521759"
sftp_conn_internal_id = "sftp_useast2"
client_time_export_path = "/Sandbox/Time Data to Workday/Time Block/"
internal_time_export_path = "/Sandbox/Time Data to Workday/Time Block/"
client_timeoff_export_path = "/Sandbox/Time Data to Workday/Time Off/"
internal_timeoff_export_path = "/Sandbox/Time Data to Workday/Time Off/"


tenant_email = 'Amit.tiwari@eisneramper.com, Richa.sinha@eisneramper.com, sap.integration.support@eisneramper.com, sap.proserv.support@eisneramper.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

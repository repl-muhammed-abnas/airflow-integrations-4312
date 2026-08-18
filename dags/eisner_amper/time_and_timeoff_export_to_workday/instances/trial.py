#pylint: disable=wildcard-import unused-wildcard-import
from eisner_amper.time_and_timeoff_export_to_workday.config import *

instance = "trial"
company_key = 'eisnerampertrial02'
replicon_conn_id = 'eisnerampertrial02_replicon_radmin'
sftp_conn_id = "sftp_useast2"
sftp_conn_internal_id = "sftp_useast2"
client_time_export_path = "/TimeBlock/"
internal_time_export_path = "/Production/Time Data to Workday/Time Block/"
client_timeoff_export_path = "/TimeOff/"
internal_timeoff_export_path = "/Production/Time Data to Workday/Time Off/"


tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

disabled=True

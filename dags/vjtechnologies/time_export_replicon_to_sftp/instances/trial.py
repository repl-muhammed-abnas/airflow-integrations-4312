# pylint: disable=wildcard-import unused-wildcard-import
from vjtechnologies.time_export_replicon_to_sftp.config import *

region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'VJTechnologiestrial01'
sftp_conn_id = 'sftp_useast2'

user_name = "replicon.admin"

replicon_conn_id = 'VJTechnologiestrial01_replicon_admin'

to_email = "kventura@vjt.com, axsupport@vjt.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

csv_filepath = "/Replicon UAT"

schedule_interval='30 22 * * 2'
schedule_time_zone = 'EST'

can_run_batch_task_var_name = f'vjtechnologiestrial01_time_export_can_run_batch_task_{instance}'

EXCLUDE_COMPANY_SLUG_LIST = ["vj-imaging-technologies-limited"]

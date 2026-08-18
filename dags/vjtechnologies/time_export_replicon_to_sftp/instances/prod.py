# pylint: disable=wildcard-import unused-wildcard-import
from vjtechnologies.time_export_replicon_to_sftp.config import *

region = 'us-east-1'
instance = "prod"
environment = 'production'
company_key = 'VJTechnologies'
sftp_conn_id = 'VJTechnologies_sftp_RepliconSFTP'

user_name = "replicon.admin"

replicon_conn_id = 'VJTechnologies_replicon_admin'

to_email = "kventura@vjt.com, axsupport@vjt.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

csv_filepath = "/Replicon"

schedule_interval='30 22 * * 2'
schedule_time_zone = 'EST'

can_run_batch_task_var_name = f'VJTechnologies_time_export_can_run_batch_task_{instance}'

EXCLUDE_COMPANY_SLUG_LIST = ["vj-imaging-technologies-limited"]
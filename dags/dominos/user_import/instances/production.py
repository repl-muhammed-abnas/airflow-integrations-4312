# pylint: disable=wildcard-import unused-wildcard-import
from dominos.user_import.config import *

instance = 'production'
environment = 'production'
company_key = 'dominospizza'

replicon_conn_id = 'DominosPizza_replicon_adminr'

tenant_email = 'Dist_ISCore_ServiceNow@dominos.com,Deepak.Mitra@dominos.com,lavanya.yasani@dominos.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'DominosPizza_sftp_replicon'
secondary_sftp_conn_id = 'DominosPizza_sftp_22407'

can_run_batch_task_var_name = f'dominospizza_user_import_{instance}_can_run_batch_task'

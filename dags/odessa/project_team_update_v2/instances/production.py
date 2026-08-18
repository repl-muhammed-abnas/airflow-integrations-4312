# pylint: disable=wildcard-import unused-wildcard-import
from odessa.project_team_update_v2.config import *

instance = 'prod'
region = 'us-east-1'
environment = 'production'

company_key = 'Odessa'

end_date = "12/31/2099"  # mm/dd/yyyy
filepath = '/jirasync'
schedule_interval = '0 */1 * * *'
pacific_timezone = 'America/Los_Angeles'
master_dag_max_active_runs = 1
child_dag_process_wbs_max_active_runs = 14
second_master_dag_max_active_runs=6

replicon_conn_id = "odessa-replicon-admin"
sftp_conn_id = "odessa-integration-useast"
http_conn_id = 'odessa_jira'

error_email = 'pavadeyya.kabburrnath@odessainc.com,replicon@odessainc.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
tenant_email = '{{ var.value.dagrun_failure_alert_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
is_update_custom_field_in_jira= True


#The folder name for this integration is incorrect and this is a to do task
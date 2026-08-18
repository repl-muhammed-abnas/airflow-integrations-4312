# pylint: disable=wildcard-import unused-wildcard-import
from odessa.project_team_update_v2.config import *

instance = 'uat'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'odessasandbox'

filepath = '/Odessa/Jira/logs'

replicon_conn_id = "odessasandbox_replicon_admin"
sftp_conn_id = "rsftp-useast_for_testing"
http_conn_id = 'odessa_jira'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'
is_update_custom_field_in_jira= True
disabled = True

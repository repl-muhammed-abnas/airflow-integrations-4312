# pylint: disable=wildcard-import unused-wildcard-import
from odessa.project_team_update_v2.config import *

instance = 'trial'
region = 'us-east-1'
environment = 'pre-production'

company_key = 'odessaafmig'

filepath = '/Odessa/Jira/logs'

replicon_conn_id = "odessaafmig-replicon-admin"
sftp_conn_id = "rsftp-useast_for_testing"
http_conn_id = 'odessa_jira'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

is_update_custom_field_in_jira= False
disabled = True

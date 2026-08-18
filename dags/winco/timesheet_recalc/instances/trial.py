# pylint: disable=wildcard-import unused-wildcard-import
from winco.timesheet_recalc.config import *

company_key = 'Wincoafmig'
hmac_secret = 'airflow_connector_ui_hmac_secret'
instance = 'trial'
environment = 'pre-production'

sftp_conn_id = 'sftp_useast2'

replicon_conn_id = 'Wincoafmig_replicon_admin'
log_filepath = '/reportextract/'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

report_name = 'Replicon to Spectrum - For Integration'

disabled=True

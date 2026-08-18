# pylint: disable=wildcard-import unused-wildcard-import
from winco.timesheet_recalc.config import *
environment = 'production'
company_key = 'Winco'

instance = 'production'

hmac_secret = 'airflow_connector_ui_hmac_secret'
sftp_conn_id = 'Winco_sftp_qkroOiFM7iR4'

replicon_conn_id = 'Winco_replicon_admin'
log_filepath = '/reportextract/'

alert_email = '{{ var.value.dagrun_failure_alert_email }}'

report_name = 'Replicon to Spectrum - For Integration'

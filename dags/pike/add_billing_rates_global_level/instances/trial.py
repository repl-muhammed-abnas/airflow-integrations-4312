# pylint: disable=wildcard-import unused-wildcard-import
from pike.add_billing_rates_global_level.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'pikeafmig'

replicon_conn_id = 'pikeafmig_replicon_admin'
sftp_conn_id = 'rsftp-useast_for_testing'

input_filepath = "/PIKETrial/Pike.billingrate/Input"
archive_filepath = "/PIKETrial/Pike.billingrate/Archive"
email_id_path = "/PIKETrial/Pike.billingrate/fromaddress"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True

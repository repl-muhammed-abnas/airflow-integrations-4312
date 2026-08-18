# pylint: disable=wildcard-import unused-wildcard-import
from macquariegroup.sftp_custom_notification.config import *

instance = "trial"

company_key = "macquarieproductiontrial01"

#runs every day from 10th to 15th of each month
master_dag_schedule_interval = "0 5 10-15 * *"
master_dag_active_runs = 1

# Clients SFTP
sftp_conn_id = "Airflow_migration_SFTP_eucentral"
replicon_conn_id = "macquarieproductiontrial01-replicon-tuser"

# Trial will be changed to Sandbox for UAT
input_filepath = "/macquarie/Reconciliation import/Trial/Input"
archive_filepath = "/macquarie/Reconciliation import/Trial/Archive"

timezone = "Australia/Sydney"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
disabled = True

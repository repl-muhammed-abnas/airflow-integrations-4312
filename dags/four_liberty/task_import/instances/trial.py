# pylint: disable=wildcard-import unused-wildcard-import
from four_liberty.task_import.config import *

instance = 'trial'
company_key = "4Libertyafmig"

sftp_conn_id = "Airflow_migration_SFTP_useast2"
sftp_conn_id2 = "Airflow_migration_SFTP_useast2"
replicon_conn_id = "4Libertyafmig-replicon-dataintegration"

input_filepath = "/4liberty/input"
processing_filepath = "/4liberty/processing"
archive_filepath = "/4liberty/input/archive"
reference_filepath = "/4liberty/reference"
reference_archive_filepath = "/4liberty/reference/archive"
log_filepath = "/4liberty/logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f'four_liberty_task_import_{instance}_can_run_batch_task'
# disabled = True

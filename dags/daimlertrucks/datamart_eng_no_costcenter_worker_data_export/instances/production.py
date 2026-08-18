# pylint: disable=wildcard-import unused-wildcard-import
from daimlertrucks.datamart_eng_no_costcenter_worker_data_export.config import *

instance = "production"
environment = 'production'
master_dag_id = f'daimlertrucks_datamart_eng_no_costcenter_worker_data_export_master_{instance}'

company_key = 'daimlertrucks'
replicon_conn_id = 'daimlertrucks_replicon_replicon'
sftp_conn_id = 'sftp_daimlertrucks_540697'

input_filepath = "/Production/Datamart/ENG/Worker/RejectedRecords"
archive_filepath = "/Production/Datamart/ENG/Archive"

can_run_batch_task_var_name = f'{instance}_datamart_eng_no_costcenter_export_can_run_batch_task'

tenant_email = "Replicon-Support@daimlertruck.com,dtna-eng-timewiz@daimlertruck.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

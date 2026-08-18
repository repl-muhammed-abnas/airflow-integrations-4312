# pylint: disable=wildcard-import unused-wildcard-import
from daimlertrucks.datamart_eng_no_costcenter_worker_data_export.config import *

instance = "trial"
environment = 'pre-production'
master_dag_id = f'daimlertrucks_datamart_eng_no_costcenter_worker_data_export_master_{instance}'

company_key = 'DaimlerTrucksafmig'
replicon_conn_id = 'daimlertrucksafmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'

input_filepath = "/DaimlerTrucksafmig/Datamart/ENG/Worker/RejectedRecords"
archive_filepath = "/DaimlerTrucksafmig/Datamart/ENG/Archive"

can_run_batch_task_var_name = f'{instance}_datamart_eng_no_costcenter_export_can_run_batch_task'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'
# disabled = True

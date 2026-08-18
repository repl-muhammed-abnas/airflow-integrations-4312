#pylint: disable=wildcard-import unused-wildcard-import
from nttdatabc.udf_update.config import *
region = 'us-east-1'
instance = "trial"
environment = 'pre-production'
company_key = 'NTTDATABCafmig'
replicon_conn_id = 'nttdatabcafmig_replicon_admin'
sftp_conn_id = 'sftp_useast2'

max_active_runs_child = 1
manual_master_dag_interval = 1

input_filepath = '/NTTDATABC/Workato_Mapper/'

internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

can_run_batch_task = f'nttdata_seniority_udf_update_can_run_batch_task_{instance}'

upload_filepath = '/NTTDATABC/Seniority_UDF_Update/'
disabled = True

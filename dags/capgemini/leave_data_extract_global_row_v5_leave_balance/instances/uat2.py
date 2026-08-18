# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_row_v5_leave_balance.config import *
from capgemini.leave_data_extract_global_row_v5_leave_balance.mappers.region_country_mapper_uat2 import REGION_COUNTRY_MAPPER as MAPPER

instance = 'uat2'
environment = 'pre-production'
version = 'v5'

company_key = 'capgeminiuat2'

replicon_conn_id = 'capgeminiuat2_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiUAT'
pgp_conn_id = 'pgp_capgeminiuat2'

input_filepath = "/Outbound/LeaveBalanceUAT2/Input"
s3_upload_filepath = "CapgeminiUAT/Outbound/LeaveBalanceUAT2/Input"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

leave_balance_export_master_dag_id = f'capgemini_leave_balance_export_master_{instance}_{version}'
leave_balance_export_child_dag_id = f'capgemini_leave_balance_export_child_{instance}_{version}'

REGION_COUNTRY_MAPPER = MAPPER

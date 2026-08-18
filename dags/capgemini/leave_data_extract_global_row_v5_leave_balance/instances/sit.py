# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_row_v5_leave_balance.config import *
from capgemini.leave_data_extract_global_row_v5_leave_balance.mappers.region_country_mapper_sit import REGION_COUNTRY_MAPPER as MAPPER

instance = 'sit'
environment = 'pre-production'
version = 'v5'

company_key = 'capgeminisit'

replicon_conn_id = 'capgeminisit_replicon_leave_data.integration'
sftp_conn_id = 'sftp_capgemini_502546_CapgeminiSIT'
pgp_conn_id = 'pgp_capgeminisit'

input_filepath = "/Outbound/LeaveBalance/Input"
s3_upload_filepath = "CapgeminiSIT/Outbound/LeaveBalance/Input"

tenant_email = 'groupitrepliconsupportl2@capgemini.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }},capgeminisupportreplicon@deltek.com'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

leave_balance_export_master_dag_id = f'capgemini_leave_balance_export_master_{instance}_{version}'
leave_balance_export_child_dag_id = f'capgemini_leave_balance_export_child_{instance}_{version}'

REGION_COUNTRY_MAPPER = MAPPER

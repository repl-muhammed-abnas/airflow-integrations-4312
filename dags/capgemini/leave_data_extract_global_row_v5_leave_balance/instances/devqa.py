# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.leave_data_extract_global_row_v5_leave_balance.config import *
from capgemini.leave_data_extract_global_row_v5_leave_balance.mappers.region_country_mapper_devqa import REGION_COUNTRY_MAPPER as MAPPER

instance = 'devqa'
environment = 'pre-production'
version = 'v5'

company_key = 'capgeminidev'

replicon_conn_id = 'capgeminidev_replicon_leave_data.integration'
sftp_conn_id = 'rsftp-useast_for_testing'
pgp_conn_id = 'pgp_capgeminidev'

input_filepath = "/Outbound/LeaveBalance/Input"
s3_upload_filepath = "CapgeminiDev/Outbound/LeaveBalance/Input"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f'capgemini_leave_data_extract_can_run_batch_task_{instance}'

leave_balance_export_master_dag_id = f'capgemini_leave_balance_export_master_{instance}_{version}'
leave_balance_export_child_dag_id = f'capgemini_leave_balance_export_child_{instance}_{version}'

REGION_COUNTRY_MAPPER = MAPPER

disabled = True

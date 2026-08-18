# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.psa_resource_assignment_v2.config import *

instance = 'dxctrial01'
version = "_v2"

company_key = 'dxctrial01'
environment = 'pre-production'

replicon_conn_id = 'dxctrial01'
sftp_conn_id = 'sftp_useast2'
pgp_conn_id = 'pgp_dxcsandbox_psa_resource_assignment'

input_filepath = '/rit_test/psa_resource/Input'
archive_filepath = '/rit_test/psa_resource/Archive'
log_filepath = '/rit_test/psa_resource/Logs'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_internal_testing_email }}'

dag_id_postfix = f'{instance}{version}'

master_dagid = f'dxctechnology_psa_resource_assignment_master_{dag_id_postfix}'
process_each_wbs_dagid = f'dxctechnology_psa_resource_process_parent_wbs_{dag_id_postfix}'
process_child_wbs_dagid = f'dxctechnology_psa_resource_process_each_child_wbs_{dag_id_postfix}'
process_date_range_child_dagid = f'dxctechnology_psa_resource_date_range_child_{dag_id_postfix}'
process_assignment_child_dagid = f'dxctechnology_psa_resource_assignment_child_{dag_id_postfix}'

# Variable names
can_run_batch_task_var_name = f'psa_resource_assignment_batch_task_{dag_id_postfix}'
can_decrypt_file_var_name = f'psa_resource_assignment_can_decrypt_file_{dag_id_postfix}'

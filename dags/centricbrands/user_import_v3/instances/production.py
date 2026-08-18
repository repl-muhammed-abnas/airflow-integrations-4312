# pylint: disable=wildcard-import unused-wildcard-import
from centricbrands.user_import_v3.config import *
from centricbrands.user_import_v3.mappers.centric_brands_time_off_type_assignment_mapper import (
    centric_brands_time_off_type_assignment)
from centricbrands.user_import_v3.mappers.centric_brands_time_off_policy_starting_policy_mapper import (
    centric_brands_time_off_policy_starting_policy_mapper)
from centricbrands.user_import_v3.mappers.centric_brands_time_off_policy_starting_policy_china_hongkong_mapper import (
    centric_brands_time_off_policy_starting_policy_china_hongkong)

region = 'us-east-1'
instance = 'production'
environment = 'production'

company_key = 'CentricBrands'
replicon_conn_id = 'centricbrands_replicon_admin'

tenant_email = "laurenbrown@centricbrands.com,simprote@centricbrands.com,dlewis@centricbrands.com,jsable@centricbrands.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

sftp_conn_id = 'sftp_centricbrands_655328'

input_filepath = '/UserSync/Input'
reference_filepath = '/UserSync/reference/'
log_filepath = '/UserSync/logs/'
archive_filepath = '/UserSync/Archived/'

can_run_batch_task = f'centricbrands_user_import_can_run_batch_task_{instance}'

version = 'v3'

master_dag_id = f'centricbrands_user_import_master_{instance}_{version}'

child_add_user_dag_id = f'centricbrands_user_import_add_user_child_{instance}_{version}'
child_update_user_dag_id = f'centricbrands_user_import_update_user_child_{instance}_{version}'
child_disable_user_dag_id = f'centric_brands_workflow_to_disable_user_child_{instance}_{version}'
child_assign_supervisor_dag_id = f'centricbrands_user_import_assign_supervisor_child_{instance}_{version}'

child_locations_teams_add_dag_id = f'centricbrands_user_import_locations_teams_add_child_{instance}_{version}'
child_department_add_dag_id = f'centricbrands_user_import_department_add_child_{instance}_{version}'
child_cost_centers_locations_add_dag_id = f'centricbrands_user_import_cost_centers_locations_add_child_{instance}_{version}'

child_add_user_time_off_dag_id = f'centricbrands_user_import_add_user_time_off_child_{instance}_{version}'

child_rehire_user_time_off_type_assignment_dag_id = f'centricbrands_user_import_rehire_user_time_off_type_assignment_child_{instance}_{version}'
child_rehire_user_time_off_policy_assignment_dag_id = f'centricbrands_user_import_rehire_user_time_off_policy_assignment_child_{instance}_{version}'

child_update_user_time_off_type_assignment_dag_id = f'centricbrands_user_import_update_user_time_off_type_assignment_child_{instance}_{version}'
child_update_user_time_off_policy_assignment_dag_id = f'centricbrands_user_import_update_user_time_off_policy_assignment_child_{instance}_{version}'
child_put_0_balance_dag_id = f'centricbrands_user_import_put_0_balance_child_{instance}_{version}'

TO_TYPE_ASSIGNMENT_MAPPER = centric_brands_time_off_type_assignment
TO_POLICY_STARTING_BALANCE_MAPPER = centric_brands_time_off_policy_starting_policy_mapper
TO_POLICY_STARTING_BALANCE_CHINA_HK_MAPPER = centric_brands_time_off_policy_starting_policy_china_hongkong

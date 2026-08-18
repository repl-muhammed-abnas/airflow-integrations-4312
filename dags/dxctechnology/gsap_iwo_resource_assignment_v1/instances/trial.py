# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.gsap_iwo_resource_assignment_v1.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = 'dxctrial01'
replicon_conn_id = 'dxctrial01'

dag_id_postfix = f'{instance}_v1'

can_run_batch_task_var_name = f'dxctechnology_gsap_iwo_resource_{instance}_can_run_batch_task'

master_dag_id =f'dxctechnology_gsap_iwo_resource_assignment_master_{dag_id_postfix}'
process_wbs_dag_id = f'dxctechnology_gsap_iwo_resource_process_wbs_{dag_id_postfix}'
process_c1_compass_assignment_dag_id = f'dxctechnology_gsap_iwo_resource_c1_compass_assignment_{dag_id_postfix}'
process_gsap_assignment_dag_id = f'dxctechnology_gsap_iwo_resource_gsap_assignment_{dag_id_postfix}'
process_gsap_resource_dag_id = f'dxctechnology_gsap_iwo_resource_assign_resource_task_{dag_id_postfix}'
process_reprocess_batch_dag_id = f'dxctechnology_gsap_iwo_resource_assignment_reprocess_batch_{dag_id_postfix}'
move_to_processing_master_dag_id = f'dxctechnology_gsap_iwo_resource_assignment_move_file_processing_master_{dag_id_postfix}'
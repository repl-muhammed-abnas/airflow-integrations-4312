# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.workday_user_import.user_import.config import *
from dxctechnology.workday_user_import.user_import.mappers.master_mapper_v2 import MAPPER

instance = "trial"

version = "v0"

environment = "pre-production"
can_run_batch_task_var_name = f"dxctechnology_workday_user_import_can_run_batch_task_variable_{instance}"

company_key = "dxctrial01"
replicon_conn_id = "dxctrial01_replicon_x.replicon.workday1"
sftp_conn_id = "sftp_useast2"

pgp_conn_id = "dxctechnology_workday_user_import_pgp_connection"

input_file_path = "/WD/Input/local"
philippines_file_path = "WD/Input/PHL"
hungary_file_path = "/WD/Input/HUN/"
uki_csc_file_path = "/WD/Input/UKI_CSC/"
uki_es_file_path = "/WD/Input/UKI_ES/"
archive_file_path = "/WD/Archives"
log_file_path = "/WD/Logs"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
bcc_emails = "{{ var.value.dagrun_internal_testing_email }}"

can_decrypt_file_var_name = f'dxctechnology_workday_user_sync_can_decrypt_file_{instance}'

DXC_WORKDAY_USER_SYNC_USER_MAPPER = MAPPER

process_time_off_accrual = f"dxctechnology_workday_user_sync_timeoff_assignment_policy_update_for_no_accrual_child_{instance}_{version}"

process_log_generation_dagid = f"dxctechnology_workday_user_sync_process_log_generation_{instance}_{version}"

workday_user_import_main_dag = f"dxctechnology_workday_user_sync_master_{instance}_{version}"
workday_user_import_process_groups_udfs_dag = f"dxctechnology_workday_user_import_process_groups_udf_child_{instance}_{version}"
workday_user_import_process_schedule_creation_dag = f"dxctechnology_workday_user_import_process_schedule_creation_child_{instance}_{version}"
workday_user_import_process_location_creation_dag = f"dxctechnology_workday_user_import_process_location_creation_child_{instance}_{version}"
workday_user_import_process_supervisor_assignment = f"dxctechnology_workday_user_import_process_supervisor_assignment_child_{instance}_{version}"

# Global Users dag_id's
workday_user_import_process_gbl_data_child_dag = f"dxctechnology_workday_user_import_process_gbl_data_child_dag_{instance}_{version}"
workday_user_import_global_users_add_user_child_dag = f"dxctechnology_workday_user_import_global_users_add_user_child_{instance}_{version}"
workday_user_import_global_users_update_user_child_dag = f"dxctechnology_workday_user_import_global_users_update_user_child_{instance}_{version}"

workday_user_import_process_gsap_data_child_dag = f"dxctechnology_workday_user_import_process_gsap_data_child_dag_{instance}_{version}"
workday_user_import_portugal_process_users_child_dag = f"dxctechnology_workday_user_import_process_portugal_process_user_child_dag_{instance}_{version}"

workday_user_import_process_portugal_data_child_dag = f"dxctechnology_workday_user_import_process_portugal_data_child_dag_{instance}_{version}"

workday_user_import_process_users_child_dag_dag_ids_per_erp = {
    erp: f"dxctechnology_workday_user_import_{erp}_process_users_child_{instance}_{version}" for erp in DXC_ERPS}

workday_user_import_global_users_update_user_child_dag = f"dxctechnology_workday_user_import_global_users_update_user_child_{instance}_{version}"
workday_user_import_global_users_add_user_child_dag = f"dxctechnology_workday_user_import_global_users_add_user_child_{instance}_{version}"

workday_user_import_australia_users_update_user_child_dag = f"dxctechnology_workday_user_import_australia_users_update_user_child_{instance}_{version}"
workday_user_import_australia_users_add_user_child_dag = f"dxctechnology_workday_user_import_australia_users_add_user_child_{instance}_{version}"

ADD_UPDATE_CHILD_DAGS_PER_ERP = {
    "global": {
        "add": workday_user_import_global_users_add_user_child_dag,
        "update": workday_user_import_global_users_update_user_child_dag
    },
    "gsap": {
        "add": workday_user_import_australia_users_add_user_child_dag,
        "update": workday_user_import_australia_users_update_user_child_dag
    }
}

workday_user_import_process_canada_data_child_dag = f"dxctechnology_workday_user_import_process_canada_data_child_dag_{instance}_{version}"
workday_user_import_process_canada_users_child_dag = f"dxctechnology_workday_user_import_process_canada_process_user_child_dag_{instance}_{version}"

# Costa Rica
workday_user_import_process_costa_rica_data_child_dag = f"dxctechnology_workday_user_import_process_costa_rica_data_child_dag_{instance}_{version}"
workday_user_import_costa_rica_process_users_child_dag = f"dxctechnology_workday_user_import_costa_rica_process_users_child_{instance}_{version}"

# USA LES
workday_user_import_process_usa_les_data_child_dag = f"dxctechnology_workday_user_import_process_us_les_data_child_dag_{instance}_{version}"
usa_les_process_users_child_dag_id = f"dxctechnology_workday_user_import_usa_les_process_users_child_{instance}_{version}"

# INDIA
workday_user_import_process_india_data_child_dag = f"dxctechnology_workday_user_import_process_india_data_child_{instance}_{version}"
workday_user_import_india_process_users_child_dag = f"dxctechnology_workday_user_import_india_process_users_child_{instance}_{version}"

# IA Changes
workday_user_import_ia_zero_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_ia_zero_timeoff_assignment_child_{instance}_{version}"
workday_user_import_ia_one_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_ia_one_timeoff_assignment_child_{instance}_{version}"

# USA CSC
workday_user_import_process_usa_csc_data_child_dag = f"dxctechnology_workday_user_import_process_us_csc_data_child_{instance}_{version}"
usa_csc_process_users_child_dag_id = f"dxctechnology_workday_user_import_us_csc_process_users_child_{instance}_{version}"

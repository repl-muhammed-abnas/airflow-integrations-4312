# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.workday_user_import_v1.user_import.config import *
from dxctechnology.workday_user_import_v1.user_import.mappers.master_mapper import MAPPER

instance = "trial"

version = "v1"
aus_specific_version = "v3"
usa_les_specific_version = "v3"
usa_csc_specific_version = "v3"
costa_rica_specific_version = "v3"
canada_specific_version = "v2"
# Opts this instance into process_canada_users_v2.py for Canada, suffix is v2 as process_canada_users.py already holds v1
canada_data_version = "v2"
portugal_specific_version = "v2"
global_specific_version = "v2"
india_specific_version = "v2"


environment = "pre-production"
can_run_batch_task_var_name_master = f"dxctechnology_workday_user_import_master_can_run_batch_task_variable_{instance}"

company_key = "dxctrial01"
replicon_conn_id = "dxctrial01_replicon_x.replicon.workday1"
sftp_conn_id = "sftp_useast2"

pgp_conn_id = "dxctechnology_workday_user_import_pgp_connection"

input_file_path = "/WD/Test"
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


workday_user_import_process_gsap_data_child_dag = f"dxctechnology_workday_user_import_process_gsap_data_child_dag_{instance}_{version}"
workday_user_import_portugal_process_users_child_dag = f"dxctechnology_workday_user_import_process_portugal_process_user_child_dag_{instance}_{portugal_specific_version}"

workday_user_import_process_portugal_data_child_dag = f"dxctechnology_workday_user_import_process_portugal_data_child_dag_{instance}_{version}"

workday_user_import_process_users_child_dag_dag_ids_per_erp = {
    'global': f"dxctechnology_workday_user_import_global_process_users_child_{instance}_{version}",
    'gsap': f"dxctechnology_workday_user_import_australia_process_users_child_{instance}_{aus_specific_version}"
}

# Global Users dag_id's
workday_user_import_process_gbl_data_child_dag = f"dxctechnology_workday_user_import_process_gbl_data_child_dag_{instance}_{version}"

# Global Users add/update child DAG IDs
workday_user_import_global_users_add_user_child_dag = f"dxctechnology_workday_user_import_global_users_add_user_child_{instance}_{global_specific_version}"
workday_user_import_global_users_update_user_child_dag = f"dxctechnology_workday_user_import_global_users_update_user_child_{instance}_{global_specific_version}"

workday_user_import_australia_users_update_user_child_dag = f"dxctechnology_workday_user_import_australia_users_update_user_child_{instance}_{aus_specific_version}"
workday_user_import_australia_users_add_user_child_dag = f"dxctechnology_workday_user_import_australia_users_add_user_child_{instance}_{aus_specific_version}"

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

# Canada
workday_user_import_process_canada_data_child_dag = f"dxctechnology_workday_user_import_process_canada_data_child_dag_{instance}_{version}"
workday_user_import_process_canada_data_child_dag_v2 = f"dxctechnology_workday_user_import_process_canada_data_child_dag_{instance}_{canada_data_version}"
workday_user_import_process_canada_users_child_dag = f"dxctechnology_workday_user_import_process_canada_process_user_child_dag_{instance}_{canada_specific_version}"

# Costa Rica
workday_user_import_process_costa_rica_data_child_dag = f"dxctechnology_workday_user_import_process_costa_rica_data_child_dag_{instance}_{version}"
workday_user_import_costa_rica_process_users_child_dag = f"dxctechnology_workday_user_import_costa_rica_process_users_child_{instance}_{costa_rica_specific_version}"

# USA LES
workday_user_import_process_usa_les_data_child_dag = f"dxctechnology_workday_user_import_process_us_les_data_child_dag_{instance}_{version}"
usa_les_process_users_child_dag_id = f"dxctechnology_workday_user_import_usa_les_process_users_child_{instance}_{usa_les_specific_version}"

# INDIA
workday_user_import_process_india_data_child_dag = f"dxctechnology_workday_user_import_process_india_data_child_{instance}_{india_specific_version}"
workday_user_import_india_process_users_child_dag = f"dxctechnology_workday_user_import_india_process_users_child_{instance}_{india_specific_version}"

# IA Changes
workday_user_import_ia_zero_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_ia_zero_timeoff_assignment_child_{instance}_{version}"
workday_user_import_ia_one_timeoff_assignment_child_dag = f"dxctechnology_workday_user_import_ia_one_timeoff_assignment_child_{instance}_{version}"

# USA CSC
workday_user_import_process_usa_csc_data_child_dag = f"dxctechnology_workday_user_import_process_us_csc_data_child_{instance}_{version}"
usa_csc_process_users_child_dag_id = f"dxctechnology_workday_user_import_us_csc_process_users_child_{instance}_{usa_csc_specific_version}"

# Cleanup child DAG ID for disabled users
delete_future_entries_child_dag_id = f"dxctechnology_workday_user_sync_delete_future_entries_child_{instance}_v2"

# SFTP paths for location-specific file uploads
philippines_file_path = "/WD/Input/PHL/"
hungary_file_path = "/WD/Input/HUN/"
uki_csc_file_path = "/WD/Input/UKI_CSC/"
uki_es_file_path = "/WD/Input/UKI_ES/"

# Overrides: bump max_active_runs to 10 for this environment (base defaults in config.py are unchanged, so production is not affected)
global_update_user_timeoff_assignment_max_active_runs = 10
process_log_generation_max_active_runs = 10
process_time_off_accrual_mac_active_runs = 10
process_users_max_active_runs = 10

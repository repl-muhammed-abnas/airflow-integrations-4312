"""TRIAL instance configuration"""
# pylint: disable=wildcard-import unused-wildcard-import
from crl.office_schedule_import_v1.config import *

instance = "trial"
environment = "pre-production"

company_key = "CharlesRiverLaboratoriestrial01"
replicon_conn_id = "charlesriverlaboratoriestrial01_replicon_repliconadmin"
sftp_conn_id = "sftp_useast2"

log_filepath = "/CRL/office_schedule_sync/trial"

# Email Configuration
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# Variable Names
can_run_batch_task_var_name = f'crl_office_schedule_import_{instance}_can_run_batch_task'

version = "_v1"  # _v1, _v2, etc.
dag_id_suffix = f"{instance}{version}"

# DAG IDs
master_dag_id = f'crl_office_schedule_import_master_{dag_id_suffix}'
create_schedule_dag_id = f'crl_office_schedule_import_create_schedule_child_{dag_id_suffix}'
process_log_generation_dag_id = f'crl_office_schedule_import_process_log_generation_child_{dag_id_suffix}'

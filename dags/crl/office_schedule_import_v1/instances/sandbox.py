"""Sandbox instance configuration"""
# pylint: disable=wildcard-import unused-wildcard-import
from crl.office_schedule_import_v1.config import *

instance = "sandbox"
environment = "pre-production"

company_key = "CharlesRiverLaboratoriesSandbox"
replicon_conn_id = "charlesriverlaboratoriessandbox_repliconint_userimport"
sftp_conn_id = "sftp_charlesriverlaboratoriessandbox_603355"

log_filepath = "/Test/Inbound/Time Off Schedule/Logs"

# Email Configuration
tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,prabhav.potluri@crl.com'
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

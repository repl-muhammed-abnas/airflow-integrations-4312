"""Production instance configuration"""
# pylint: disable=wildcard-import unused-wildcard-import
from crl.office_schedule_import_v1.config import *

instance = "prod"
environment = "production"

company_key = "CharlesRiverLaboratories"
replicon_conn_id = "CharlesRiverLaboratories_replicon_Repliconint_userimport"
sftp_conn_id = "sftp_charlesriverlaboratories_603355"

log_filepath = "/Production/Inbound/Time Off Schedule/Log"

# Email Configuration
tenant_email = 'Bal-hr@crl.com,Valerie.McGrath@crl.com,Sean.Cotto@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,prabhav.potluri@crl.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

# Variable Names
can_run_batch_task_var_name = f'crl_office_schedule_import_{instance}_can_run_batch_task'

version = "_v1"  # _v1, _v2, etc.
dag_id_suffix = f"{instance}{version}"

# DAG IDs
master_dag_id = f'crl_office_schedule_import_master_{dag_id_suffix}'
create_schedule_dag_id = f'crl_office_schedule_import_create_schedule_child_{dag_id_suffix}'
process_log_generation_dag_id = f'crl_office_schedule_import_process_log_generation_child_{dag_id_suffix}'

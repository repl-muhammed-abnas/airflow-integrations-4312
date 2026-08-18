# pylint: disable=wildcard-import unused-wildcard-import
from seaspanshipyards.users_supervisor_details_export.config import *

instance = 'trial'
environment = 'pre-production'

company_key = 'Seaspanshipyardssb'

replicon_conn_id = 'seaspanshipyardssb_replicon_rnadmin'
sftp_conn_id = 'sftp_internal'
pgp_conn_id = 'pgp_seaspanshipyardssb_supervisor_data_export'

output_filepath = "/SeaspanshipyardsTrial/REPLICON/inbound/empAssignSupervisor/stage/"

sftp_conn_id_internal = 'sftp_internal'
reference_filepath = "/SeaspanshipyardsTrial/empAssignSupervisor/Reference/Input/"
archive_reference_filepath = "/SeaspanshipyardsTrial/empAssignSupervisor/Reference/Archive/"
ref_file_name = "ReferenceFile_SeaspanShipyardsUsersSupervisorDetails.csv"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f"seaspanshipyards_users_supervisor_details_export_can_run_batch_task_{instance}"
master_dagid = f"seaspanshipyards_users_supervisor_details_export_master_{instance}"

can_use_reference_file = f"seaspanshipyards_users_supervisor_details_export_can_use_reference_file_{instance}"
disabled = True

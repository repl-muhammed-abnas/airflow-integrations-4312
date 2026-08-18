# pylint: disable=wildcard-import unused-wildcard-import
from victoriashipyards.users_supervisor_details_export.config import *

instance = 'sandbox'
environment = 'pre-production'

company_key = 'seaspanvslsb'

replicon_conn_id = 'seaspanvslsb_replicon_repliconint'
sftp_conn_id = 'sftp_seaspanshipyards_ora_replicon_sftp_user'
pgp_conn_id = 'pgp_seaspanvslsb_supervisor_data_export'

output_filepath = "/inbound/replicon/emp_assign_supervisor/stage/"

sftp_conn_id_internal = 'sftp_internal'
reference_filepath = "/SeaspanvslSB/empAssignSupervisor/Reference/Input/"
archive_reference_filepath = "/SeaspanvslSB/empAssignSupervisor/Reference/Archive/"
ref_file_name = "ReferenceFile_VictoriaShipyardsUsersSupervisorDetails.csv"

tenant_email = "devesh.sharma@seaspan.com,Stephanie.lefort@seaspan.com,Jaime.Ortega@seaspan.com,Ashok.Pamu@seaspan.com,Chase.huber@seaspan.com"
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f"victoriashipyards_users_supervisor_details_export_can_run_batch_task_{instance}"
master_dagid = f"victoriashipyards_users_supervisor_details_export_master_{instance}"

can_use_reference_file = f"victoriashipyards_users_supervisor_details_export_can_use_reference_file_{instance}"

# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.psa_organizational_unit.config import *

instance = "production"
region = 'us-east-2'
environment = 'production'

company_key = 'dxctechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntPSA'
sftp_conn_id = 'sftp_dxctechnology_628172_PSA'
pgp_conn_id = 'pgp_psa_resource_assignment'

input_filepath = "/Production/Inbound/Org Unit/Input"
archive_filepath = "/Production/Inbound/Org Unit/Archive"
log_filepath = "/Production/Inbound/Org Unit/Logs"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

dxctechnology_psa_organizational_unit_master = f'dxctechnology_psa_organizational_unit_master_{instance}'
dxctechnology_psa_process_organizational_units_child = f'dxctechnology_psa_process_organizational_units_child_{instance}'
can_run_batch_task_var_name = f'dxctechnology_psa_organizational_unit_{instance}_can_run_batch_task'

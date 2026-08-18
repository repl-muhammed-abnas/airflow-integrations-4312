# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.time_export.psa_outbound.config import *
from dxctechnology.time_export.master_config.instances.production import *

instance = 'production'
environment = 'production'
company_key = 'DXCTechnology'
replicon_conn_id = 'dxctechnology-replicon-RepliconIntPSA'
sftp_conn_id = 'sftp_dxctechnology_628172_PSA'
pgp_conn_id = 'pgp_dxctechnology_psa_outbound'

output_filepath = '/Production/Outbound/Time Export'
s3_upload_filepath = "Timeexport/PSA"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

psa_acknowledgement_email = 'dxcintegrationlogsreplicon@deltek.com'


time_data_posting_mapper = f'dxc_time_data_posting_mapper_{instance}'
timeoff_types_to_exclude = f'dxc_compass_time_export_timeoff_types_to_exclude_{instance}'
timeoff_types_to_export = f'dxc_gsap_time_export_timeoff_types_to_export_{instance}'

max_active_dag_runs = 1
max_active_child_dag_runs = 1

bucket_name = 'replicon-integrations-dxctechnology'

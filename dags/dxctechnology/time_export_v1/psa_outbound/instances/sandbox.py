# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.time_export_v1.psa_outbound.config import *
from dxctechnology.time_export_v1.master_config.instances.sandbox import *

instance = 'sandbox'
version = 'v1'
environment = 'pre-production'
company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntPSA'
sftp_conn_id = 'sftp_dxcsandbox_628172_PSA'
pgp_conn_id = 'pgp_dxcsandbox_psa_outbound'

output_filepath = 'Test/Outbound/Time Export'
s3_upload_filepath = "Timeexport/PSA"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

psa_acknowledgement_email = '{{ var.value.dagrun_internal_testing_email }}'


time_data_posting_mapper = f'dxc_time_data_posting_mapper_{instance}_{version}'
timeoff_types_to_exclude = f'dxc_psa_time_export_timeoff_types_to_exclude_{instance}_{version}'
timeoff_types_to_export = f'dxc_gsap_time_export_timeoff_types_to_export_{instance}_{version}'

max_active_dag_runs = 1
max_active_child_dag_runs = 1

bucket_name = 'replicon-integrations-dxcsandbox'

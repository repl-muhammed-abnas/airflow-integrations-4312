# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.time_export_v1.psa_outbound.config import *
from dxctechnology.time_export_v1.master_config.instances.trial import *

instance = 'trial'
version = 'v1'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01_replicon_RepliconIntPSA'
sftp_conn_id = 'rsftp-useast_for_testing'
psa_http_conn_id = f'dxctechnology_psa_time_export_http_{instance}'
pgp_conn_id = 'pgp_dxctrial01_psa_outbound'

output_filepath = 'DXCTrial01/Test/Outbound/Time Export'
s3_upload_filepath = "Timeexport/PSA"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

psa_acknowledgement_email = '{{ var.value.dagrun_internal_testing_email }}'


time_data_posting_mapper = f'dxc_time_data_posting_mapper_{instance}_{version}'
timeoff_types_to_exclude = f'dxc_psa_time_export_timeoff_types_to_exclude_{instance}_{version}'
timeoff_types_to_export = f'dxc_gsap_time_export_timeoff_types_to_export_{instance}_{version}'

max_active_dag_runs = 1
max_active_child_dag_runs = 1

bucket_name = 'replicon.integration_dxcafmig_s3_bucket'

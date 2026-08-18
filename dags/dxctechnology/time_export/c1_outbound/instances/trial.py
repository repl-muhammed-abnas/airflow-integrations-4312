# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.time_export.c1_outbound.config import *
from dxctechnology.time_export.master_config.instances.trial import *

instance = 'trial'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01_replicon_RepliconIntC1'
sftp_conn_id = 'rsftp-useast_for_testing'
c1_http_conn_id = f'dxctechnology_c1_time_export_http_{instance}'

c1_reg_time_export_file_format = "Time Export - Master"
psa_reg_time_export_file_format = "PSA Time Export - C1 and Compass"
c1_iwo_time_export_file_format = "Time Export Master - GSAP Compass C1"

reg_schedule_interval = "0 0,6,12,18 * * *"
reg_pta_weekly_schedule_interval = "0 6 * * SUN"
iwo_schedule_interval = "0 1,5,9,13,17,21 * * *"
iwo_pta_weekly_schedule_interval = "0 21 * * SUN"

skip_run_weekday = 0
reg_skip_run_hour = 6
iwo_skip_run_hour = 21

output_filepath = 'DXCTrial01/Test/Outbound/C1TimeExtract'
s3_upload_filepath = "Timeexport/C1"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

c1_acknowledgement_email = '{{ var.value.dagrun_internal_testing_email }}'


time_data_posting_mapper = f'dxc_time_data_posting_mapper_{instance}'

max_active_dag_runs = 1
max_active_child_dag_runs = 1

bucket_name = 'replicon.integration_dxcafmig_s3_bucket'

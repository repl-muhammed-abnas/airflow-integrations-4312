# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.time_export_v1.compass_outbound.config import *
from dxctechnology.time_export_v1.master_config.instances.trial import *

instance = 'trial'
version = 'v1'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01_replicon_RepliconIntCompass'
sftp_conn_id = 'rsftp-useast_for_testing'
compass_http_conn_id = f'dxctechnology_compass_time_export_http_{instance}'

compass_reg_time_export_file_format = "Time Export - Master"
psa_reg_time_export_file_format = "PSA Time Export - C1 and Compass"
compass_iwo_time_export_file_format = "Time Export Master - GSAP Compass C1"

compass_iwo_time_export_hours_file_format = "Time Export Master - GSAP Compass C1 - Hours"

reg_schedule_interval = "0 */2 * * *"
reg_pta_weekly_schedule_interval = "0 6 * * SUN"
iwo_schedule_interval = "0 3,7,11,15,19 * * *"
iwo_pta_weekly_schedule_interval = "0 15 * * SUN"

skip_run_weekday = 0
reg_skip_run_hour = 6
iwo_skip_run_hour = 15

output_filepath = 'DXCTrial01/Test/Outbound/COMPASSTimeExtract'
s3_upload_filepath = "Timeexport/COMPASS"

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

compass_acknowledgement_email = '{{ var.value.dagrun_internal_testing_email }}'
cwf_ftp_acknowledgement_email = '{{ var.value.dagrun_internal_testing_email }}'


time_data_posting_mapper = f'dxc_time_data_posting_mapper_{instance}_{version}'
timeoff_types_to_exclude = f'dxc_compass_time_export_timeoff_types_to_exclude_{instance}_{version}'
timetype_standby_units_to_exclude = f'dxc_compass_time_export_timetype_standby_units_to_exclude_{instance}_{version}'

max_active_dag_runs = 1
max_active_child_dag_runs = 1

bucket_name = 'replicon.integration_dxcafmig_s3_bucket'

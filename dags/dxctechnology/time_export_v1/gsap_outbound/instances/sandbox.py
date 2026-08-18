# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.time_export_v1.gsap_outbound.config import *
from dxctechnology.time_export_v1.master_config.instances.sandbox import *

instance = 'sandbox'
version = 'v1'
environment = 'pre-production'
company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
sftp_conn_id = 'dxcsandbox-sftp-628172_GSAP'
gsap_http_conn_id = 'dxcsandbox_POQ_GSAPTimeData'

gsap_c1_cp_time_export_file_format = "Time Export Master - GSAP Compass C1"
master_time_export_file_format = "Time Export - Master"

gsap_c1_cp_time_export_hours_file_format = "Time Export Master - GSAP Compass C1 - Hours"

reg_schedule_interval = "0 */2 * * *"
reg_pta_weekly_schedule_interval = "0 6 * * SUN"
iwo_schedule_interval = "45 3,7,11,15,19,23 * * *"
iwo_pta_weekly_schedule_interval = "45 15 * * SUN"

skip_run_weekday = 0
reg_skip_run_hour = 6
iwo_skip_run_hour = 15

output_filepath = '/Test/Outbound'
s3_upload_filepath = "Timeexport/GSAP"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

gsap_acknowledgement_email = '{{ var.value.dagrun_internal_testing_email }}'


time_data_posting_mapper = f'dxc_time_data_posting_mapper_{instance}_{version}'
timeoff_types_to_export = f'dxc_gsap_time_export_timeoff_types_to_export_{instance}_{version}'

max_active_dag_runs = 1
max_active_child_dag_runs = 1

bucket_name = 'replicon-integrations-dxcsandbox'

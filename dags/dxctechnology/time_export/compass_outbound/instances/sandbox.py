# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.time_export.compass_outbound.config import *
from dxctechnology.time_export.master_config.instances.sandbox import *

instance = 'sandbox'
environment = 'pre-production'
company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntCompass'
sftp_conn_id = 'dxcsandbox-sftp-628172_Compass'
compass_http_conn_id = 'dxcsandbox_POQ_CompassTimeData'

compass_reg_time_export_file_format = "Time Export - Master"
psa_reg_time_export_file_format = "PSA Time Export - C1 and Compass"
compass_iwo_time_export_file_format = "Time Export Master - GSAP Compass C1"

reg_schedule_interval = "0 */2 * * *"
reg_pta_weekly_schedule_interval = "0 6 * * SUN"
iwo_schedule_interval = "0 3,7,11,15,19 * * *"
iwo_pta_weekly_schedule_interval = "0 15 * * SUN"

skip_run_weekday = 0
reg_skip_run_hour = 6
iwo_skip_run_hour = 15

output_filepath = 'Test/Outbound/COMPASSTimeExtract'
s3_upload_filepath = "Timeexport/COMPASS"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

compass_acknowledgement_email = 'compasshrtimeitl4@dxc.com'
cwf_ftp_acknowledgement_email = 'mytimefunc@dxc.com'


time_data_posting_mapper = f'dxc_time_data_posting_mapper_{instance}'
timeoff_types_to_exclude = f'dxc_compass_time_export_timeoff_types_to_exclude_{instance}'

max_active_dag_runs = 1
max_active_child_dag_runs = 1

bucket_name = 'replicon-integrations-dxcsandbox'

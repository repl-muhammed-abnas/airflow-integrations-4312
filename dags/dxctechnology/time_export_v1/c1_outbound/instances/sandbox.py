# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.time_export_v1.c1_outbound.config import *
from dxctechnology.time_export_v1.master_config.instances.sandbox import *

instance = 'sandbox'
version = 'v1'
environment = 'pre-production'
company_key = 'DXCSandbox'
replicon_conn_id = 'dxcsandbox-replicon-RepliconIntC1'
sftp_conn_id = 'dxcsandbox-sftp-628172_C1'
c1_http_conn_id = 'dxcsandbox_POQ_C1TimeData'

c1_reg_time_export_file_format = "Time Export - Master"
psa_reg_time_export_file_format = "PSA Time Export - C1 and Compass"
c1_iwo_time_export_file_format = "Time Export Master - GSAP Compass C1"

c1_iwo_time_export_hours_file_format = "Time Export Master - GSAP Compass C1 - Hours"

reg_schedule_interval = "0 0,6,12,18 * * *"
reg_pta_weekly_schedule_interval = "0 6 * * SUN"
iwo_schedule_interval = "0 1,5,9,13,17,21 * * *"
iwo_pta_weekly_schedule_interval = "0 21 * * SUN"

skip_run_weekday = 0
reg_skip_run_hour = 6
iwo_skip_run_hour = 21

output_filepath = 'Test/Outbound/C1TimeExtract'
s3_upload_filepath = "Timeexport/C1"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

c1_acknowledgement_email = 'mytimefunc@dxc.com'


time_data_posting_mapper = f'dxc_time_data_posting_mapper_{instance}_{version}'

timeoff_types_to_exclude = f'dxc_c1_time_export_timeoff_types_to_exclude_{instance}_{version}'
timetype_standby_units_to_exclude = f'dxc_c1_time_export_timetype_standby_units_to_exclude_{instance}_{version}'

max_active_dag_runs = 1
max_active_child_dag_runs = 1

bucket_name = 'replicon-integrations-dxcsandbox'
c1_ack_batch_task_var_name = f'dxc_c1_acknowledgement_batch_task_enable_{instance}'
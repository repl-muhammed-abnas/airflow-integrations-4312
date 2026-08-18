# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.time_export.gsap_outbound.config import *
from dxctechnology.time_export.master_config.instances.production import *

instance = 'production'
environment = 'production'
company_key = 'DXCTechnology'
replicon_conn_id = 'dxctechnology_replicon_RepliconIntGSAP'
sftp_conn_id = 'dxctechnology-sftp-628172_GSAP'
gsap_http_conn_id = 'dxctechnology_POP_GSAPTimeData'

gsap_c1_cp_time_export_file_format = "Time Export Master - GSAP Compass C1"
master_time_export_file_format = "Time Export - Master"

reg_schedule_interval = "0 0,6,12,18 * * *"
reg_pta_weekly_schedule_interval = "0 6 * * SUN"
iwo_schedule_interval = "30 4,10,16,22 * * *"
iwo_pta_weekly_schedule_interval = "30 10 * * SUN"

skip_run_weekday = 0
reg_skip_run_hour = 6
iwo_skip_run_hour = 10

output_filepath = '/Production/Outbound'
s3_upload_filepath = "Timeexport/GSAP"

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

gsap_acknowledgement_email = 'dxcintegrationlogsreplicon@deltek.com'


time_data_posting_mapper = f'dxc_time_data_posting_mapper_{instance}'
timeoff_types_to_export = f'dxc_gsap_time_export_timeoff_types_to_export_{instance}'

max_active_dag_runs = 1
max_active_child_dag_runs = 1

bucket_name = 'replicon-integrations-dxctechnology'

from tsystems.time_export_to_sap_v1.config import *
from tsystems.time_export_to_sap_v1.mappers.export_schedule_mapper import export_schedule_mapper

environment = "production"

instance = 'prod'
company_key = 'Tsystems'

replicon_conn_id = 'tsystems_replicon_repliconint.exports'
sftp_conn_id = 'sftp_tsystems_Replicon_ICM'

upload_filepath = "/PROD/OUT/SAP Time Export/EXPORT"
log_filepath = "/PROD/OUT/SAP Time Export/LOGS"

# T-Systems SAP integration service account
tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

version = "v1"

tsystems_dag = f'tsystems_time_export_to_sap_master_{instance}_{version}'
timeexport_to_sap_child_dag = f'tsystems_time_export_to_sap_child_{instance}_{version}'

EXPORT_SCHEDULE_MAPPER = export_schedule_mapper

tsystem_mapper_variable = f"tsystems_timeexport_to_sap_mapper_{instance}"
can_use_variable_mapper = f"tsystems_timeexport_can_use_variable_mapper_{instance}"

schedule_interval = "30 18 * * *"  # Run daily at 18:30 UTC every day

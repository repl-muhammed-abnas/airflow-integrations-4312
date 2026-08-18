from tsystems.time_export_to_sap.config import *
from tsystems.time_export_to_sap.mappers.export_schedule_mapper import export_schedule_mapper

region = 'eu-central-1'
environment = "production"

instance = 'prod'
company_key = 'Tsystems'

replicon_conn_id = 'tsystems_replicon_repliconint.exports'
sftp_conn_id = 'sftp_tsystems_Replicon_ICM'

upload_filepath = "/PROD/OUT/SAP Time Export/EXPORT"
log_filepath = "/PROD/OUT/SAP Time Export/LOGS"

tenant_email = "TSI_Replicon@t-systems.com"
internal_logs_email = '{{ var.value.dagrun_internal_log_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

tsystems_dag = f'tsystems_time_export_to_sap_master_{instance}'
timeexport_to_sap_child_dag = f'tsystems_time_export_to_sap_child_{instance}'

EXPORT_SCHEDULE_MAPPER = export_schedule_mapper

tsystem_mapper_variable = f"tsystems_timeexport_to_sap_mapper_{instance}"
can_use_variable_mapper = f"tsystems_timeexport_can_use_variable_mapper_{instance}"

schedule_interval = "30 19 * * *"  # Run daily at 19:30 UTC every day

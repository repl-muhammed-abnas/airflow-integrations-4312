from tsystems.time_export_to_sap_v1.config import *
from tsystems.time_export_to_sap_v1.mappers.export_schedule_mapper_qa import export_schedule_mapper_qa

instance = 'trial'
company_key = 'TsystemsSB'

replicon_conn_id = 'tsystems_replicon_replicon.admin'
sftp_conn_id = 'sftp_useast2'

upload_filepath = "/TsystemsSB/timedata_export/export_data"
log_filepath = "/TsystemsSB/timedata_export/logs"

tenant_email ='{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
alert_email = "{{ var.value.dagrun_internal_testing_email }}"

version = "v1"

tsystems_dag = f'tsystems_time_export_to_sap_master_{instance}_{version}'
timeexport_to_sap_child_dag = f'tsystems_time_export_to_sap_child_{instance}_{version}'

EXPORT_SCHEDULE_MAPPER = export_schedule_mapper_qa

tsystem_mapper_variable = f"tsystems_timeexport_to_sap_mapper_{instance}"
can_use_variable_mapper = f"tsystems_timeexport_can_use_variable_mapper_{instance}"

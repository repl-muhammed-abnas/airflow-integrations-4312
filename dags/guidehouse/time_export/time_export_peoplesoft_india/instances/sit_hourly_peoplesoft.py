# pylint: disable=wildcard-import unused-wildcard-import
from guidehouse.time_export.time_export_peoplesoft_india.config import *
from guidehouse.time_export.time_export_master.mapper.timeoff_project_task_mapper_v1 import (
    timeoff_project_task_mapper,
)

instance = "sit"
company_key = "GuideHouseIncSB2"
replicon_conn_id = "replicon_guidehouse_repliconint"
pgp_conn_id = "guidehousesb2_replicon_pgp_conn"
sftp_conn_id = "sftp_guidehousesb2_678659_uat"

fs="peoplesoft"
financial_system = "PeopleSoft"
run_type = "hourly"
schedule_interval = None


master_dag_id = f"guidehouse_time_export_hourly_master_{fs}_{instance}"
ps_export_dag_id = f"guidehouse_time_export_{fs}_export_child_{instance}"
dl_ps_export_dag_id = f"guidehouse_time_export_datalake_approved_data_master_{instance}"
TIMEOFF_PROJECT_TASK_MAPPER = timeoff_project_task_mapper
tenant_email = "guidehousedeltekprojectteam@deltek.com,ghcostpoint@guidehouse.com"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
ps_outbound_path_trial = "/SIT/Outbound/PPS Time Export"
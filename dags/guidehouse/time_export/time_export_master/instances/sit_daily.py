# pylint: disable=wildcard-import unused-wildcard-import
from guidehouse.time_export.time_export_master.config import *
from guidehouse.time_export.time_export_master.mapper.timeoff_project_task_mapper_v1 import timeoff_project_task_mapper
instance = "sit"
company_key = "GuideHouseIncSB2"
replicon_conn_id = "replicon_guidehouse_repliconint"
pgp_conn_id = "guidehousesb2_replicon_pgp_conn"
sftp_conn_id = "sftp_guidehousesb2_678659_uat"
run_type = "daily"
schedule_interval = "0 19 * * *"

master_dag_id = f"guidehouse_time_export_master_{run_type}_{instance}"
ps_child_dag_id = f"guidehouse_time_export_{run_type}_master_peoplesoft_{instance}"
india_child_dag_id = f"guidehouse_time_export_{run_type}_master_india_{instance}"
dl_cp_export_dag_id = f"guidehouse_time_export_datalake_approved_data_cp_child_{instance}"



TIMEOFF_PROJECT_TASK_MAPPER=timeoff_project_task_mapper
dl_outbound_path="/SIT/Outbound/DataLake Time Export"
env_suffix = f"{instance.upper()}"
tenant_email = "guidehousedeltekprojectteam@deltek.com,ghcostpoint@guidehouse.com"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"


from guidehouse.time_export.time_extract_to_datalake_unapproved_time.config import *
from guidehouse.time_export.time_export_master.mapper.timeoff_project_task_mapper import timeoff_project_task_mapper

# Instance configuration
company_key = "GuideHouseIncSB"
instance = "uat"

# Airflow connections
replicon_conn_id = f"{company_key}-replicon-replicon.int"
pgp_conn_id = "guidehousesb2_replicon_pgp_conn"
sftp_conn_id = "sftp_guidehousesb2_678659_uat"

# SFTP details
sftp_remote_filepath = "/UAT/Outbound/DataLake Unapproved Time Export"

# Version
version = "" # _v1, _v2, "" for initial version
dag_suffix = f"{instance}{version}"

# Child DAGs
process_time_extract_child_dag_id = f"guidehouse_time_extract_to_datalake_unapproved_time_process_child_{dag_suffix}"

# Mapper
TIME_OFF_PROJECT_TASK_MAPPER = timeoff_project_task_mapper

### Country specific configurations
# Time zone
timezone = usa_timezone

# Country
country = "usa"

# Master DAGs
master_dag_id = f"guidehouse_time_extract_to_datalake_unapproved_time_{country}_master_{dag_suffix}"

# Schedule
schedule_interval = usa_schedule_interval

# Company code
COMPANY_CODE = [USA_COMPANY_CODE_SET_1, USA_COMPANY_CODE_SET_2, USA_COMPANY_CODE_SET_3]
# Email recipients
tenant_email = "guidehousedeltekprojectteam@deltek.com,ghcostpoint@guidehouse.com"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"

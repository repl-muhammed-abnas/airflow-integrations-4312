# pylint: disable=wildcard-import unused-wildcard-import
from guidehouse.time_export.time_export_peoplesoft_india.config import *
from guidehouse.time_export.time_export_master.mapper.timeoff_project_task_mapper_v1 import (
    timeoff_project_task_mapper,
)

instance = "trial"
company_key = "GuideHouseIncSB2"
replicon_conn_id = "replicon_guidehouse_repliconint"

pgp_conn_id = "guidehouse_pgp"

sftp_conn_id = "sftp_useast2"
financial_system = "India"
fs="india"
run_type = "daily"
schedule_interval = None

master_dag_id = f"guidehouse_time_export_daily_master_{fs}_{instance}"
ps_export_dag_id = f"guidehouse_time_export_{fs}_export_child_{instance}"
dl_ps_export_dag_id = f"guidehouse_time_export_datalake_approved_data_master_{instance}"

TIMEOFF_PROJECT_TASK_MAPPER = timeoff_project_task_mapper
tenant_email = "{{ var.value.dagrun_internal_testing_email }}"
internal_logs_email = "{{ var.value.dagrun_internal_testing_email }}"
ps_outbound_path_trial = "Trial/Outbound/PPS Time Export"
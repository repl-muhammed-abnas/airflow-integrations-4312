# pylint: disable=wildcard-import unused-wildcard-import
from datetime import timedelta
from cie_infosys.ts_approval_utility_v2.config import *

instance = 'sandbox'
region = 'us-east-1'
environment = 'pre-production'
company_key = 'EisnerAmperSandbox'
replicon_conn_id = "replicon_infosys_sandbox"

tenant_email = "richa.sinha@eisneramper.com,PradipKumar@deltek.com,AnishHiralikar@deltek.com,AravindGalipalli@deltek.com"
internal_logs_email = "ashishtiwari@deltek.com,PradipKumar@deltek.com,AnishHiralikar@deltek.com,AravindGalipalli@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

bucket_name = "replicon-airflow-dev-cie-group"
file_path = "EisnerAmperSandbox/infosys_timehseet_approval/long_running_task"
file_name = "LongRunningTask.csv"

status_artifacts_file_path = "EisnerAmperSandbox/infosys_timehseet_approval"
ts_status_artifacts_file_name = "TimesheetArtifacts.csv"
entry_status_artifacts_file_name = "EntriesArtifacts.csv"

max_master_run = 1
max_child_run = 5
chunk_size = 200
schedule_interval = "0 * * * *" #timedelta(minutes=master_dag_interval)
timezone = "America/New_York"

can_run_batch_task_var_name = f'{company_key}_timesheet_approval_{instance}_can_run_batch_task'.lower(
)
#disabled = True

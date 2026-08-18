# pylint: disable=wildcard-import unused-wildcard-import
from cie_randstadlifescience.timeDataExport.config import *
region = 'us-east-1'
environment = 'pre-production'
instance = "trial"
company_key = 'randstadtrial02'
replicon_conn_id = 'randstadtrial02'

sftp_conn_id = "randstadtrialSFTP"
sftp_filepath = "/Trial/Time Data Extract/"
export_filename = "RPL_Solutions_Time_"

tenant_email = 'PradipKumar@deltek.com'
internal_email = "PradipKumar@deltek.com,ashishtiwari@deltek.com,AnjaliMer@deltek.com"
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

bucket_name = "replicon-airflow-dev-cie-group"
file_path = "randstadtrial02/TimeDataExport"
file_name = "timesheet_uris.csv"
excludedTS_file_name = "ExcludedTimesheetDetails.csv"

timedata_report_name = 'TimeDataExport_1.0'
audit_report_name = "TimesheetAuditTrailColumnsForTimeDataExport"
entrydata_report_name = "TimeEntryDataForTimeExport"

schedule_interval = "30,00 7,13 * * TUE,WED,THU,FRI"
time_zone = "America/New_York"
trigger_time = [
    "Tuesday 7:00", "Tuesday 13:00", "Wednesday 7:30", "Thursday 7:30", "Friday 7:30"
]


can_run_batch_task_var_name = f'{company_key}_timedata_export_{instance}_can_run_batch_task'

disable = True

disabled = True

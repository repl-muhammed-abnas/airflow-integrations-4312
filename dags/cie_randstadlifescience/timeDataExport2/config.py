region = 'us-east-1'
environment = 'pre-production'

processing_frequency_minutes = 120
post_batch_size = 10000
tenant_email = 'ashishtiwari@deltek.com'
internal_logs_email = 'ashishtiwari@deltek.com'
timedata_report_name = 'TimeDataExport_1.0'
audit_report_name = "TimesheetAuditTrailColumnsForTimeDataExport"
entrydata_report_name = "TimeEntryDataForTimeExport"
debug = False
dag_max_active_tasks = 128
execution_timeout_days = 14
lastrunlogfilepath = "/"
static_columns = {
    "SOURCE": "PAS",
    "EXPENSE_TYPE": "",
    "RNA_EXPENSE_DATE": "",
    "RNA_EXP_PAY_AMT": "",
    "SP_EXP_APPROVER": "",
    "APPROVAL_STATUS": 2,
    "RNA_TASK_BILLABLE": 2,
    "RNA_TSH_BILLABLE": 2,
    "RNA_RPL_NEW_TIME": "Y",
    "PROCESS_STATUS": "N",
    "RECORD_IDENTIFIER": "T",
    "EMPLID2": "",
    "FIRST_NAME_SRCH": "",
    "LAST_NAME_SRCH": ""
}

team_id = "CIE"
max_child_run = 5

instance_tz = "America/New_York"
trigger_time = [
    "Tuesday 7:00", "Tuesday 13:00", "Wednesday 7:30", "Thursday 7:30", "Friday 7:30"
]

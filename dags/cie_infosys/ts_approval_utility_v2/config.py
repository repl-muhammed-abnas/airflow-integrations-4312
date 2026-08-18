region = 'us-east-1'
environment = 'pre-production'
company_key = 'eisnerampertrial01'
replicon_conn_id = "replicon_infosys_trial"
team_id = "CIE"


tenant_email = "ashishtiwari@deltek.com"
internal_logs_email = "ashishtiwari@deltek.com"
infosys_config = {
    "entry_report_name":  "***BaseReport_TimeEntryDetails",
    "timesheet_report_name":  "***BaseReport_TimesheetDetails",
    "period_in_months": 6,
    "last_modiefied_in_hours": 24,
    "timesheet_approve_remarks": "Timesheet Approved by Approval Utility.",
    "entry_approve_remarks": "Time Entry Approved by Approval Utility.",
}


timezone = 'America/New_York'
master_dag_interval = 10
execution_timeout_days = 14
schedule_time = "23:00"  # 24 Hours format
india_sub_string = ["(IN)"]
location = ""

chunk_size = 100

can_debug_test_data = False  # warning only for local testing

region = 'us-east-1'
environment = 'pre-production'
company_key = 'epiqsystemsinctrial01'
replicon_conn_id = "replicon_epiq_trial"
team_id = "CIE"

tenant_email = "PradipKumar@deltek.com,RahulGajeli@deltek.com"
internal_logs_email = "PradipKumar@deltek.com,RahulGajeli@deltek.com"
infosys_config = {
    "timesheet_report_name":  "***BaseReport_TimesheetDetails",
    "timesheet_approve_remarks": "Timesheet Approved by Approval Utility.",
}

timezone = 'America/New_York'
master_dag_interval = 10
execution_timeout_days = 14
schedule_time = "20:00"  # 24 Hours format
run_type = ""

chunk_size = 100

can_debug_test_data = False  # warning only for local testing

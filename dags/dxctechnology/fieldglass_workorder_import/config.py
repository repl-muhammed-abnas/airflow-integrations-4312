

region = "us-east-2"
environment = "pre-production"
master_max_active_run = 1
max_active_child_runs = 5
max_active_process_child_runs = 5
sftp_time_out = 10
keyNamespace_compass = "DXC_WorkOrderRateTypeRates"
keyNamespace_gsap = "CWF_workorderdetails"
user_list_report = "User list for purchase and worker order - Replicon"
expected_report_columns = """User Name,Login Name,Employee ID,CWF C1 alternate ID,UserUri,User Status,\
Employee Type (Current),Timesheet Template,Timesheet Approval Path,Work Week,validation,C1 Purchase Order,\
Work Order ID,Timesheet Period (Current)"""
schedule_interval = "*/30 * * * *"
execution_timeout=14

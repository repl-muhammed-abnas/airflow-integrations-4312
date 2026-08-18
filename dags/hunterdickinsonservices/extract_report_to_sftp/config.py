region = 'us-east-1'
environment = 'production'


max_active_runs = 1
execution_timeout_days = 14
schedule_interval_houly = '*/30 6-15 * * *'
schedule_interval_daily = '20 3 * * *'
time_zone = 'PST8PDT'

extract_report_name_hourly = "10_DB Sync_RP_TimeSheetData_Last_Hourly"
extract_report_hourly_file_name = "HDSI_DB Sync_RP_TimeSheetData_Last_Hourly"
extract_report_hourly_file_path = "/Hourly/"

extract_report_name_timesheetdata_daily = "9_DB Sync_RP_TimeSheetData_Last_Daily"
extract_report_timesheet_daily_file_name = "HDSI_DB Sync_RP_TimeSheetData_Last_Daily"
extract_report_daily_file_path = "/Daily/"

extract_report_name_timesheetdata_current_daily = "8_DB Sync_RP_TimeSheetData_Current_Daily"
extract_report_timesheet_Current_daily_file_name = "HDSI_DB Sync_RP_TimeSheetData_Current_Daily"

extract_report_name_userid_daily = "7_DB Sync_UserId_Dept"
extract_report_userid_daily_file_name = "HDSI_DB Sync_UserId_Dept"

extract_report_name_useractivitycode_daily = "6_DB Sync_UserActivityCode"
extract_report_useractivitycode_daily_file_name = "HDSI_DB Sync_UserActivityCode"

extract_report_name_user_daily = "5_DB Sync_User"
extract_report_user_daily_file_name = "HDSI_DB Sync_User"

extract_report_name_project_daily = "4_DB Sync_Project"
extract_report_project_daily_file_name = "HDSI_DB Sync_Project"

extract_report_name_primarydepartment_daily = "3_DB Sync_PrimaryDepartment"
extract_report_primarydepartment_daily_file_name = "HDSI_DB Sync_PrimaryDepartment"

extract_report_name_clients_daily = "2_DB Sync_Clients"
extract_report_clients_daily_file_name = "HDSI_DB Sync_Clients"

extract_report_name_department_daily = "1_DB Sync_Department"
extract_report_department_daily_file_name = "HDSI_DB Sync_Department"

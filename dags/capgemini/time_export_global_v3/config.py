region = 'eu-central-1'
environment = 'pre-production'

max_active_runs = 1
max_active_time_export_child = 4
schedule_interval = "0 0,4,8,12,16,20 * * *"
time_zone = "Etc/UTC"

sumo_conn_id = 'sumologic-exportlogger'
dagrun_log_sumo_conn_id = 'sumologic-dagrunlogger'

execution_timeout_mins_write_csv = 90
execution_timeout_days = 14

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'

export_columns = ["ProjectTime. Project Time ID","ProjectTime. BatchName","ProjectTime.GGID",
    "ProjectTime.Local Employee Number","Cost Center Name","Project. CostCenterID(PU)",
    "Market Unit Name","Market Unit Code","Employee Type Name","Employee.Employee Contract Type",
    "Location Name","Employee.Office City Code","User Status","Employee.GGID","Employee.Email",
    "Employee.People_Manager_GGID","Employee Group","Employee. EmployeeCategory",
    "Employee.Global Grade","Employee. HRBP_Manager_GGID","ProjectTime.Entrydate",
    "Timesheet Period","ProjectTime.ApprovalStatus","ProjectTime.Hours","ProjectTime.Comments",
    "ProjectTime.ProjectName","ProjectTime.ProjectID","Source System","Project.ProjectType",
    "ProjectTime.TaskName","ProjectTime.TaskCode","Billability","ProjectTime.ActivityName",
    "ProjectTime.ActivityCode","Client Name","Client Code","ProjectTime.Absence Type Name",
    "ProjectTime.Absence Type Code","Export Number","Unit of Measure","Work Location",
    "Place Of Work","Place of Work (CHE)","Place of Work (ESP)","Place of Work (FRA)","Place of Work (MAR)",
    "Postal Code","Work Location CAN","Work Location USA","Export Creation Datetime","Row Number"]

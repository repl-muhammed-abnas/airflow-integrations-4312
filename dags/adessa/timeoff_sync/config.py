region = 'eu-central-1'
environment = 'pre-production'

report_name = 'User List For Integration'
expected_report_columns = 'loginname,employeeid,useruri'

execution_timeout_days = 14
max_active_runs = 1
max_child_active_runs = 1
schedule_interval = '30 * * * *'
file_sensor_timeout = 15

s3_download_link_expiry = 7*24*60*60

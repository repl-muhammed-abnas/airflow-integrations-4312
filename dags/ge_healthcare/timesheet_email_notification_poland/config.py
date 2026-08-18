region = 'eu-central-1'
environment = 'pre-production'

company_key = 'GEafmig'

report_name = '***Email Notification Integration***'
# pylint: disable=line-too-long
expected_report_columns = 'Timesheet URI,Timesheet Period,User Name,User URI,Approval Status,User Supervisor Name (Current),User Supervisor Email address,Supervisor URI,Waiting on Approver,Location (Current)'

sso_link = "https://fss.gecompany.com/fss/idp/startSSO.ping?PartnerSpId=https://global.replicon.com/!/saml2/GE"

max_active_runs = 1
child_max_active_runs = 10
execution_timeout_days = 14
schedule_interval = "0 8 * * *"

aws_conn_id = 'replicon.workato_S3_account'
s3_bucket_name = 'replicon.integration_eu_s3_bucket'

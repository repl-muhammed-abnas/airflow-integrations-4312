# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.deleted_timeoff_booking_webhook_logging.config import *


instance = 'UAT'

environment = 'pre-production'

company_key = 'capgeminiuat'
replicon_conn_id = 'capgeminiuat_replicon_leave_data.integration'
tenant_wide_log = f"{company_key}_deleted_timeoffs_log"

s3_upload_filepath = "CapgeminiUAT/DeletedTimeoffDetails/"

aws_conn_id = 'replicon.workato_S3_account'
bucket_name = 'replicon.integration_eu_s3_bucket'

webhook_secret_var_name ="capgeminiuat_timeoff_booking_webhook_secret"
tenant_log = f"artifact:CapgeminiUAT:log:{tenant_wide_log}"
should_use_multiple_logs = True
tenant_wide_log_list = [tenant_log,
                        f"{tenant_log}_1",
                        f"{tenant_log}_2",
                        f"{tenant_log}_3",
                        f"{tenant_log}_4"
                    ]

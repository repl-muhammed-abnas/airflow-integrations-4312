# pylint: disable=wildcard-import unused-wildcard-import
from capgemini.adhoc.deleted_timeoff_export_load_poc.config import *
instance = 'dev'
leave_status = 'deleted leaves'

region = "eu-central-1"
environment = 'pre-production'

company_key = 'capgeminidev'

schedule_interval = "0 4,8,12,16,20 * * *"
schedule_interval_1am = "0 1 * * *"

replicon_conn_id = 'capgeminidev_replicon_leave_data.integration'
report_name = "GTM INT007 LeaveRequestsDeleted Test"
export_columns = ['Leave Request ID', 'Employee ID', 'Local Employee Number', 'Current Time Off Type', 'Current Start Date', 'Current End Date', 'Modified On']

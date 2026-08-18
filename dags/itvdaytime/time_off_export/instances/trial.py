# pylint: disable=wildcard-import unused-wildcard-import
from itvdaytime.time_off_export.config import *

region = 'eu-central-1'
environment = 'pre-production'
company_key = 'itvdaytimetrial01'
schedule_interval='00 01 * * *'
instance = 'itvdaytimetrial01'
max_active_runs = 5
replicon_conn_id = 'replicon-itvdaytime-radmin'
sftp_conn_id = 'sftp-itvdaytime-563217'
output_file_path = '/Trial/Export/Time Off/'
disabled = True

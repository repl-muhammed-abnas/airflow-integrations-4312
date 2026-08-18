# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_leanstaffing_assignment.config import *

instance = ''  # for trial , kept it empty to retain the old dag id with logs in QA Env
region = 'us-east-2'
environment = 'pre-production'
company_key = 'dxctrial01'
replicon_conn_id = 'replicon-dxctechnology-ftp'
sftp_conn_id = 'dxctechnology-ftp'
secondary_sftp_conn_id = 'dxctechnology-ftp'
http_conn_id = 'dxctechnology-c1-leanstaffing-export-http'
processing_frequency_minutes = 120
post_batch_size = 10000
output_filepath = '/Production/Outbound/C1LeanstaffingAssignment/Output'
archive_filepath = '/Input'
tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
extract_report_name = 'Replicon to C1 Team Assignments extract'
report_filter_name = 'UDFFilter_Project4_Taskassignment_billingratechangedate'

debug = False
disabled = True

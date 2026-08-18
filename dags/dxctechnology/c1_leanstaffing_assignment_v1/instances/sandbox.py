# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_leanstaffing_assignment_v1.config import *

instance = 'sandbox'
region = 'us-east-2'
environment = 'pre-production'

company_key = 'DXCSandbox'

replicon_conn_id = 'dxcsandbox-replicon-RepliconIntC1'
sftp_conn_id = 'dxcsandbox-sftp-628172_C1'
secondary_sftp_conn_id = 'dxcsandbox-sftp-628172_C1taskteam'
http_conn_id = 'dxcsandbox_POQ_C1Leanstaffing'

processing_frequency_minutes = 120
post_batch_size = 10000

output_filepath = '/Test/Outbound/C1LeanstaffingAssignment/Output/'
archive_filepath = '/backup'

tenant_email = 'dxcintegrationlogsreplicon@deltek.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

extract_report_name = 'Replicon to C1 Team Assignments extract'
report_filter_name = 'UDFFilter_Project4_Taskassignment_billingratechangedate'

can_send_result_var = f"dxctechnology_c1_leanstaffing_export_send_downstream_{instance}_v1"

debug = False

dag_id_postfix = f'_{instance}'

team_rate_modified_token_variable_name = f'dxctechnology_c1_leanstaffing_webhook{dag_id_postfix}_teamratemodified_secret'
team_dates_modified_token_variable_name = f'dxctechnology_c1_leanstaffing_webhook{dag_id_postfix}_teamdatesmodified_secret'
team_modified_token_variable_name = f'dxctechnology_c1_leanstaffing_webhook{dag_id_postfix}_teammodified_secret'

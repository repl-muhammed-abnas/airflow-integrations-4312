# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.c1_leanstaffing_assignment_webhook.config import *

instance = 'production'
version = "_v2"

environment = 'production'

company_key = 'DXCTechnology'

replicon_conn_id = 'dxctechnology-replicon-RepliconIntC1'

team_rate_modified_token_variable_name = f'dxctechnology_c1_leanstaffing_webhook_{instance}_teamratemodified_secret'
team_dates_modified_token_variable_name = f'dxctechnology_c1_leanstaffing_webhook_{instance}_teamdatesmodified_secret'
team_modified_token_variable_name = f'dxctechnology_c1_leanstaffing_webhook_{instance}_teammodified_secret'

# Dag ID's
webhook_master_dag_id = f'dxctechnology_c1_leanstaff_assignment_webhook_master_{instance}{version}'
webhook_processor_dag_id = f'dxctechnology_c1_leanstaff_assignment_webhook_processor_{instance}{version}'

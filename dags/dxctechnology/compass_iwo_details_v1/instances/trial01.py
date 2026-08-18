# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_iwo_details_v1.config import *

region = 'us-east-2'
environment = 'pre-production'
instance = 'dxctrial01'
company_key = 'dxctrial01'
replicon_conn_id = 'dxctrial01-replicon-RepliconIntCompass'

dag_id_postfix = f'v1_{instance}'

schedule_interval = '0 */2 * * *'
first_delta = 4
second_delta = 2

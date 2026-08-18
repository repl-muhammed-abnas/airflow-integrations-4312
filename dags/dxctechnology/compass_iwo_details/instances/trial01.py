# pylint: disable=wildcard-import unused-wildcard-import
from dxctechnology.compass_iwo_details.config import *


region = 'us-east-2'
environment = 'pre-production'
instance = 'dxctrial01'
company_key = 'dxctrial01'

dag_id_postfix = instance
schedule_interval = '0 */2 * * *'
first_delta = 4
second_delta = 2

# pylint: disable=wildcard-import unused-wildcard-import
from exponent.conflict_check_report.config import *

instance = 'trial'
environment = 'pre-production'
company_key = 'exponent'

vantagepoint_conn_id = 'exponent_inc_vantagepoint_conn_id'

dag_id = f'exponent_conflict_check_report_{instance}'
basic_auth_username_exponent_inc = f"exponent_inc_vantagepoint_conflict_check_report_webhook_username_{instance}"
basic_auth_pass_exponent_inc = f"exponent_inc_vantagepoint_conflict_check_report_webhook_pass_{instance}"
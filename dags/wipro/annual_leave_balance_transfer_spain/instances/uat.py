# pylint: disable=wildcard-import unused-wildcard-import
from wipro.annual_leave_balance_transfer_spain.config import *
from wipro.annual_leave_balance_transfer_spain.mappers.yearly_entitlement import yearly_entitlement

instance = "uat"

company_key = "Wiprosandbox2"

replicon_conn_id = "wiprosandbox2_repliconint"

master_dag = f'wipro_spain_annual_leave_balance_transfer_master_{instance}'
child_dag = f'wipro_spain_annual_leave_balance_transfer_child_{instance}'

tenant_email = 'spain.hrss@wipro.com'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'

can_run_batch_task_var_name = f"wipro_spain_annual_leave_balance_transfer_{instance}_can_run_batch_task"

YEARLY_ENTITLEMENT_MAPPER = yearly_entitlement

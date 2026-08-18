# pylint: disable=wildcard-import unused-wildcard-import
from crl.termination_export.config import *
from crl.termination_export.mapper.payroll_calendar_mapper import CANADA_PAYROLL_CALENDER_MAPPER
from crl.termination_export.mapper.regular_time_employee_types import REGULAR_TIME_EMPLOYEE_TYPES

instance = 'trial'

region = 'us-east-1'
environment = 'pre-production'

company_key = 'CharlesRiverLaboratoriestrial01'
replicon_conn_id = 'charlesriverlaboratoriestrial01_replicon_riteam'
sftp_conn_id = 'sftp_CharlesRiverLaboratoriestrial01_adp'
pgp_conn_id = 'pgp_crltrial_adp_payroll_export'
secondary_encrypted_sftp_conn_id = 'rsftp-useast_for_testing'

tenant_email = '{{ var.value.dagrun_internal_testing_email }}'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

payroll_export_file_format = 'Canada ADP Export'
export = "Yes"


max_active_runs = 1
execution_timeout_days = 14


output_filepath = '/interfaces/PQ3/GVE220/put'
secondary_encrypted_output_filepath = '/CRLTrial/interfaces/PQ3/GVE220/put'

time_zone = "US/Eastern"

schedule_interval = "0 12,18 * * *"
can_run_batch_task_var_name = f'crl_canada_payroll_export_{instance}_can_run_batch_task'
CANADA_PAYROLL_CALENDER_MAPPER_TO_USE = CANADA_PAYROLL_CALENDER_MAPPER
REGULAR_TIME_EMPLOYEE_TYPES_TO_USE = REGULAR_TIME_EMPLOYEE_TYPES

adp_gv_system = 'Q'
gv_system_number = '1'
run_dag_payroll = True

employee_type = ('Hourly_Regular_Full-Time_Project','Hourly_Regular_Full-Time','Hourly_Regular_Part-Time','Hourly_Regular_Part-Time_Project',
              'Hourly_Temporary_Full-Time','Hourly_Temporary_Full-Time_Project','Hourly_Temporary_Part-Time','Hourly_Temporary_Part-Time_Project',
              'Temporary PT Hourly')

# pylint: disable=wildcard-import unused-wildcard-import
from crl.termination_balance_export.config import *
from crl.termination_balance_export.mapper.payroll_calendar_mapper import CANADA_PAYROLL_CALENDER_MAPPER

instance = 'uat'


company_key = 'CharlesRiverLaboratoriesSandbox'
replicon_conn_id = 'charlesriverlaboratoriessandbox_repliconint_payrollexport'
sftp_conn_id = 'sftp_CharlesRiverLaboratoriestrial01_adp'
pgp_conn_id = 'pgp_crltrial_adp_payroll_export'
secondary_encrypted_sftp_conn_id = 'sftp_charlesriverlaboratoriessandbox_603355'

tenant_email = 'Sean.Cotto@crl.com,Janet.Janocha@crl.com,Padmapooshanam.Chandrasekaran@crl.com,Prasanthi.Takkellapati@crl.com,LakshmanaRao.Mandala@crl.com,SAPCPISUPPORT@charlesriverlabs.com,MTL-Payroll@crl.com,Shari.Guttman@crl.com'
internal_logs_email = '{{ var.value.dagrun_internal_testing_email }}'
alert_email = '{{ var.value.dagrun_failure_alert_email }}'

payroll_export_file_format = 'Canada ADP Export'
secondary_sftp_conn_id = 'sftp_charlesriverlaboratoriessandbox_603355'
encrypt_output_file = True
file_name_prefix = 'PQ0476'
date_time_format = "%m/%d/%Y, %H:%M:%S"

max_active_runs = 1
execution_timeout_days = 14


output_filepath = '/put/'
secondary_encrypted_output_filepath = '/Test/Outbound/Canada ADP Payroll/'
log_filepath = '/put/'

time_zone = "US/Eastern"

schedule_interval = "0 18 * * *"
can_run_batch_task_var_name = f'crl_canada_payroll_export_{instance}_can_run_batch_task'

CANADA_PAYROLL_CALENDER_MAPPER_TO_USE = CANADA_PAYROLL_CALENDER_MAPPER

adp_gv_system = 'Q'
gv_system_number = '1'

run_dag_payroll = False

vacation_timeoff = ('[CAN] Vacances/Vacation St. Constant','[CAN] Vacances 2023/Vacation 2023 Carry over',
            '[CAN] Anniversaire de service/Service Anniversary','[CAN] Vacances 2023/SC Vacation 2023 Carry over','[CAN] Vacances/Vacation 2021-2022',
            '[CAN] Vacances/Vacation May 22 - June 23','[CAN] Vacances/Vacation June 23 - Dec 23','[CAN] Vacances/Vacation 2024 (Jan 24 - Apr 9th 24)')
disabled = True